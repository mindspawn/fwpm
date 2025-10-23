from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import List, Tuple
from urllib.parse import quote_plus

from zoneinfo import ZoneInfo

from .config import AppConfig, FilterConfig, parse_filter_description
from .confluence_client import ConfluenceClient
from .issue_content import DefaultIssueContentProvider, IssueContentProvider
from .jira_client import JiraClient
from .llm_client import LLMClient
from .renderers import build_confluence_storage

logger = logging.getLogger(__name__)
_SYSTEM_PROMPT = "You are a helpful assistant that summarizes Jira issues for engineering leadership."


class Workflow:
    def __init__(
        self,
        app_config: AppConfig,
        jira_client: JiraClient,
        llm_client: LLMClient,
        confluence_client: ConfluenceClient,
        issue_content_provider: IssueContentProvider | None = None,
    ) -> None:
        self.app_config = app_config
        self.jira_client = jira_client
        self.llm_client = llm_client
        self.confluence_client = confluence_client
        self.issue_content_provider = issue_content_provider or DefaultIssueContentProvider()

    def collect_issues(self, filter_id: str, include_comments: bool = True) -> Tuple[dict, List[dict]]:
        filter_details = self.jira_client.get_filter(filter_id)

        jql = filter_details.get("jql")
        logger.info("Executing filter %s with JQL: %s", filter_id, jql)

        fields = [
            "summary",
            "description",
            "status",
            "assignee",
            "reporter",
        ]
        if include_comments:
            fields.append("comment")

        issues = self.jira_client.search_issues(
            jql=jql,
            fields=fields,
        )
        logger.info("Filter %s returned %s issues", filter_id, len(issues))
        return filter_details, issues

    def run(self, filter_id: str) -> None:
        filter_details, issues = self.collect_issues(filter_id)

        description = filter_details.get("description", "")
        filter_cfg = parse_filter_description(description, self.app_config.llm_model)

        llm_outputs = self._run_llm_round(issues, filter_cfg)
        self._publish_confluence_page(filter_id, filter_details, issues, llm_outputs, filter_cfg)

    def run_with_placeholder(self, filter_id: str) -> None:
        filter_details, issues = self.collect_issues(filter_id, include_comments=False)
        description = filter_details.get("description", "")
        filter_cfg = parse_filter_description(description, self.app_config.llm_model)
        placeholder_outputs = [(issue, "This is where the LLM response is") for issue in issues]
        self._publish_confluence_page(
            filter_id, filter_details, issues, placeholder_outputs, filter_cfg
        )

    def _publish_confluence_page(
        self,
        filter_id: str,
        filter_details: dict,
        issues: List[dict],
        llm_outputs: List[Tuple[dict, str]],
        filter_cfg: FilterConfig,
    ) -> None:
        body = build_confluence_storage(
            jira_base_url=self.app_config.jira_base_url,
            filter_id=filter_id,
            filter_name=filter_details.get("name", ""),
            total_issues=len(issues),
            issue_blocks=(
                (
                    issue["key"],
                    issue.get("fields", {}).get("summary", ""),
                    self._assignee_name(issue),
                    self._assignee_activity_url(issue),
                    output,
                )
                for issue, output in llm_outputs
            ),
        )

        confluence_cfg = filter_cfg.confluence
        result = self.confluence_client.create_page(
            space_key=confluence_cfg.space_key,
            parent_page_id=confluence_cfg.parent_page_id,
            title=confluence_cfg.page_name,
            body_storage=body,
        )
        logger.info(
            "Created Confluence page id=%s link=%s",
            result.get("id"),
            result.get("_links", {}).get("base"),
        )

    def _run_llm_round(
        self, issues: List[dict], filter_cfg: FilterConfig
    ) -> List[Tuple[dict, str]]:
        outputs: List[Tuple[dict, str]] = []
        start = time.time()

        for issue in issues:
            issue_text = self.issue_content_provider.build_issue_text(issue)
            logger.debug("Constructed issue text for %s", issue.get("key"))
            user_prompt = self._build_user_prompt(filter_cfg, issue_text)
            response_text = self.llm_client.generate_completion(
                system_prompt=_SYSTEM_PROMPT,
                issue_text=user_prompt,
            )
            outputs.append((issue, response_text))

        elapsed = time.time() - start
        logger.info(
            "Processed %s LLM requests in %.2f seconds",
            len(outputs),
            elapsed,
        )
        return outputs

    def _build_user_prompt(self, filter_cfg: FilterConfig, issue_text: str) -> str:
        now_pst = datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y-%m-%d %H:%M")
        parts = [
            filter_cfg.llm.prompt.strip(),
            f"The current date and time is {now_pst} PST.",
            "JIRA Extracted Text:",
            issue_text.strip(),
        ]
        return "\n\n".join(part for part in parts if part)

    def _assignee_name(self, issue: dict) -> str:
        assignee = (issue.get("fields") or {}).get("assignee") or {}
        return assignee.get("displayName", "Unassigned")

    def _assignee_activity_url(self, issue: dict) -> str | None:
        assignee = (issue.get("fields") or {}).get("assignee") or {}
        identifier = (
            assignee.get("accountId")
            or assignee.get("name")
            or assignee.get("key")
            or assignee.get("emailAddress")
        )
        if not identifier:
            return None
        base = self.app_config.jira_base_url.rstrip("/")
        return f"{base}/secure/ViewProfile.jspa?name={quote_plus(identifier)}#tab=activity-stream"
