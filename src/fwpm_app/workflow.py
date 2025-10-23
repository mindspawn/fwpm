from __future__ import annotations

import logging
import time
from typing import List, Tuple

from .config import AppConfig, FilterConfig, parse_filter_description
from .confluence_client import ConfluenceClient
from .issue_content import DefaultIssueContentProvider, IssueContentProvider
from .jira_client import JiraClient
from .llm_client import LLMClient
from .renderers import build_confluence_storage

logger = logging.getLogger(__name__)


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

    def run(self, filter_id: str) -> None:
        filter_details = self.jira_client.get_filter(filter_id)

        jql = filter_details.get("jql")
        description = filter_details.get("description", "")
        logger.info("Executing filter %s with JQL: %s", filter_id, jql)

        filter_cfg = parse_filter_description(description, self.app_config.llm_model)
        issues = self.jira_client.search_issues(
            jql=jql,
            fields=[
                "summary",
                "description",
                "status",
                "assignee",
                "reporter",
                "comment",
            ],
        )
        logger.info("Filter %s returned %s issues", filter_id, len(issues))

        llm_outputs = self._run_llm_round(issues, filter_cfg)
        body = build_confluence_storage(
            jira_base_url=self.app_config.jira_base_url,
            issue_blocks=(
                (
                    issue["key"],
                    issue.get("fields", {}).get("summary", ""),
                    (issue.get("fields", {}).get("assignee") or {}).get("displayName", "Unassigned"),
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
            response_text = self.llm_client.generate_completion(
                system_prompt=filter_cfg.llm.prompt,
                issue_text=issue_text,
            )
            outputs.append((issue, response_text))

        elapsed = time.time() - start
        logger.info(
            "Processed %s LLM requests in %.2f seconds",
            len(outputs),
            elapsed,
        )
        return outputs
