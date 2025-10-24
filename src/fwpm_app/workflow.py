from __future__ import annotations

import logging
import time
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional
import re
import unicodedata
from html.parser import HTMLParser
from urllib.parse import quote_plus

from zoneinfo import ZoneInfo

from .config import AppConfig, FilterConfig, parse_filter_description
from .confluence_client import ConfluenceClient
from .defaults import (
    ISSUE_TEXT_OUTPUT_DIR,
    LLM_REQUEST_DELAY_SECONDS,
    LLM_RESPONSE_OUTPUT_DIR,
    CONFLUENCE_OUTPUT_FILE,
)
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
        validate_html: bool = True,
    ) -> None:
        self.app_config = app_config
        self.jira_client = jira_client
        self.llm_client = llm_client
        self.confluence_client = confluence_client
        self.issue_content_provider = issue_content_provider or DefaultIssueContentProvider()
        self.validate_html = validate_html

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
            "priority",
            "labels",
            "created",
            "updated",
            "flagged",
        ]
        if include_comments:
            fields.append("comment")

        issues = self.jira_client.search_issues(
            jql=jql,
            fields=fields,
        )
        logger.info("Filter %s returned %s issues", filter_id, len(issues))
        return filter_details, issues

    def run(self, filter_id: str, limit: int | None = None) -> None:
        filter_details, issues = self.collect_issues(filter_id)
        if limit is not None:
            issues = issues[:limit]

        description = filter_details.get("description", "")
        filter_cfg = parse_filter_description(description, self.app_config)

        workflow_start = time.time()
        llm_outputs = self._run_llm_round(issues, filter_cfg)
        body = self._build_confluence_body(filter_id, filter_details, llm_outputs, filter_cfg)
        self._persist_confluence_body(body)
        if self.validate_html:
            self._validate_html(body)
        self._publish_confluence_page(filter_cfg, body)
        logger.info(
            "Workflow completed for filter %s in %.2f seconds",
            filter_id,
            time.time() - workflow_start,
        )

    def run_with_placeholder(self, filter_id: str, limit: int | None = None) -> None:
        workflow_start = time.time()
        filter_details, issues = self.collect_issues(filter_id, include_comments=False)
        if limit is not None:
            issues = issues[:limit]
        description = filter_details.get("description", "")
        filter_cfg = parse_filter_description(description, self.app_config)
        placeholder_outputs = []
        for issue in issues:
            hydrated_issue = self._hydrate_issue(issue["key"])
            issue_text = self._prepare_issue_text(hydrated_issue)
            user_prompt = self._build_user_prompt(filter_cfg, issue_text)
            self._persist_prompt(hydrated_issue.get("key"), user_prompt)
            placeholder_outputs.append((hydrated_issue, "This is where the LLM response is"))
        body = self._build_confluence_body(filter_id, filter_details, placeholder_outputs, filter_cfg)
        self._persist_confluence_body(body)
        if self.validate_html:
            self._validate_html(body)
        self._publish_confluence_page(filter_cfg, body)
        logger.info(
            "Placeholder workflow completed for filter %s in %.2f seconds",
            filter_id,
            time.time() - workflow_start,
        )

    def _publish_confluence_page(
        self,
        filter_cfg: FilterConfig,
        body: str,
    ) -> None:
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
        overall_start = start
        total = len(issues)

        for index, issue in enumerate(issues, start=1):
            hydrated_issue = self._hydrate_issue(issue["key"])
            issue_text = self._prepare_issue_text(hydrated_issue)
            logger.debug("Constructed issue text for %s", hydrated_issue.get("key"))
            user_prompt = self._build_user_prompt(filter_cfg, issue_text)
            self._persist_prompt(hydrated_issue.get("key"), user_prompt)
            logger.info(
                "Sending LLM prompt (%s/%s) for issue %s",
                index,
                total,
                hydrated_issue.get("key"),
            )
            prompt_start = time.time()
            response_text = self.llm_client.generate_completion(
                system_prompt=filter_cfg.llm.system_prompt,
                issue_text=user_prompt,
                temperature=filter_cfg.llm.temperature,
                top_p=filter_cfg.llm.top_p,
                frequency_penalty=filter_cfg.llm.frequency_penalty,
                presence_penalty=filter_cfg.llm.presence_penalty,
            )
            prompt_elapsed = time.time() - prompt_start
            logger.info(
                "LLM response received for %s (elapsed %.2fs)",
                hydrated_issue.get("key"),
                prompt_elapsed,
            )
            self._persist_llm_response(hydrated_issue.get("key"), response_text)
            outputs.append((hydrated_issue, response_text))
            if LLM_REQUEST_DELAY_SECONDS:
                time.sleep(LLM_REQUEST_DELAY_SECONDS)

        elapsed = time.time() - start
        logger.info(
            "Processed %s LLM requests in %.2f seconds",
            len(outputs),
            elapsed,
        )
        return outputs

    def _build_confluence_body(
        self,
        filter_id: str,
        filter_details: dict,
        llm_outputs: List[Tuple[dict, str]],
        filter_cfg: FilterConfig,
    ) -> str:
        total_issues = len(llm_outputs)
        issue_blocks = (
            (
                issue["key"],
                issue.get("fields", {}).get("summary", ""),
                self._assignee_name(issue),
                self._assignee_activity_url(issue),
                self._reporter_name(issue),
                self._priority_name(issue),
                self._labels(issue),
                self._status_name(issue),
                self._is_impediment(issue),
                response_text,
            )
            for issue, response_text in llm_outputs
        )

        return build_confluence_storage(
            jira_base_url=self.app_config.jira_base_url,
            filter_id=filter_id,
            filter_name=filter_details.get("name", ""),
            total_issues=total_issues,
            issue_blocks=issue_blocks,
        )

    def _build_user_prompt(self, filter_cfg: FilterConfig, issue_text: str) -> str:
        now_pst = datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y-%m-%d %H:%M")
        context = issue_text.strip()
        parts = [
            "Use the following context as your learned knowledge, inside <context></context> XML tags.",
            f"<context>{context}</context>",
            f"The current date and time is {now_pst} PST.",
            "When answering the user: If you don't know, just say you don't know.",
            "Avoid mentioning that you obtained the information from the context.,"
            "Answer according to the language of the user's question.",
            "Given the context information, answer the query.",
            "Query: ",
            filter_cfg.llm.prompt.strip(),
        ]
        return "\n\n".join(part for part in parts if part)

    def _prepare_issue_text(self, issue: dict) -> str:
        return self.issue_content_provider.build_issue_text(issue)

    def _persist_prompt(self, issue_key: str | None, prompt_text: str) -> None:
        if not issue_key or prompt_text is None:
            return
        directory = Path(ISSUE_TEXT_OUTPUT_DIR)
        try:
            directory.mkdir(parents=True, exist_ok=True)
        except OSError:
            logger.warning("Unable to create issue text output directory: %s", directory)
            return

        safe_key = issue_key.replace("/", "_")
        path = directory / f"{safe_key}.txt"
        try:
            path.write_text(prompt_text, encoding="utf-8")
        except OSError:
            logger.warning("Failed to persist prompt for %s at %s", issue_key, path)

    def _persist_llm_response(self, issue_key: Optional[str], response_text: str) -> None:
        if not issue_key or response_text is None:
            return
        directory = Path(LLM_RESPONSE_OUTPUT_DIR)
        try:
            directory.mkdir(parents=True, exist_ok=True)
        except OSError:
            logger.warning("Unable to create LLM response output directory: %s", directory)
            return

        safe_key = issue_key.replace("/", "_")
        path = directory / f"{safe_key}.txt"
        try:
            normalized = self._normalize_text(response_text)
            path.write_text(normalized, encoding="utf-8")
        except OSError:
            logger.warning("Failed to persist LLM response for %s at %s", issue_key, path)

    def _persist_confluence_body(self, body: str) -> None:
        if body is None:
            return
        path = Path(CONFLUENCE_OUTPUT_FILE)
        parent = path.parent
        try:
            if parent and parent != Path("."):
                parent.mkdir(parents=True, exist_ok=True)
            path.write_text(self._normalize_text(body), encoding="utf-8")
        except OSError:
            logger.warning("Failed to persist Confluence body to %s", path)

    def _normalize_text(self, text: str) -> str:
        if text is None:
            return ""
        normalized = unicodedata.normalize("NFKC", text)
        normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")
        normalized = normalized.replace("\u00a0", " ")
        normalized = normalized.replace("\u200b", "")
        normalized = re.sub(r"[^\x00-\x7F]+", "", normalized)
        return normalized

    def _validate_html(self, body: str) -> None:
        validator = _HTMLStructureValidator()
        try:
            validator.feed(body)
            validator.close()
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(f"HTML validation failed during parsing: {exc}") from exc
        if validator.errors:
            snippet = "; ".join(validator.errors[:5])
            raise RuntimeError(f"HTML validation failed: {snippet}")

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

    def _reporter_name(self, issue: dict) -> str:
        reporter = (issue.get("fields") or {}).get("reporter") or {}
        return reporter.get("displayName", "Unknown")

    def _priority_name(self, issue: dict) -> str:
        priority = (issue.get("fields") or {}).get("priority") or {}
        return priority.get("name", "None")

    def _labels(self, issue: dict) -> Tuple[str, ...]:
        labels = (issue.get("fields") or {}).get("labels") or []
        return tuple(label for label in labels if isinstance(label, str) and label)

    def _status_name(self, issue: dict) -> str:
        status = (issue.get("fields") or {}).get("status") or {}
        return status.get("name", "Unknown")

    def _is_impediment(self, issue: dict) -> bool:
        fields = issue.get("fields") or {}
        flag_field = fields.get("flagged") or []
        if isinstance(flag_field, list):
            for item in flag_field:
                if isinstance(item, dict):
                    name = item.get("name") or item.get("value") or ""
                    normalized = name.lower()
                    logger.debug(
                        "Issue %s flag entry examined: name=%s value=%s",
                        issue.get("key"),
                        item.get("name"),
                        item.get("value"),
                    )
                    if "impediment" in normalized:
                        logger.debug("Issue %s flagged as impediment via flag field", issue.get("key"))
                        return True
            if flag_field:
                logger.debug("Issue %s flagged field found but no impediment match: %s", issue.get("key"), flag_field)

        custom_field = fields.get("customfield_16801")
        if custom_field is not None:
            logger.debug(
                "Issue %s customfield_16801 value=%s",
                issue.get("key"),
                custom_field,
            )
            if self._custom_field_contains_impediment(issue, custom_field):
                return True
        status = (fields.get("status") or {}).get("name", "")
        if isinstance(status, str) and status.lower() == "impediment":
            logger.debug("Issue %s flagged as impediment via status", issue.get("key"))
            return True
        logger.debug(
            "Issue %s not marked impediment; status=%s flagged=%s",
            issue.get("key"),
            status,
            flag_field,
        )
        return False

    def _hydrate_issue(self, issue_key: str) -> dict:
        fields = [
            "summary",
            "description",
            "status",
            "assignee",
            "reporter",
            "priority",
            "labels",
            "comment",
            "created",
            "updated",
            "flagged",
            "customfield_16801",
        ]
        return self.jira_client.get_issue(
            issue_key,
            fields=fields,
            expand=["changelog"],
        )

    def _custom_field_contains_impediment(self, issue: dict, value) -> bool:
        key = issue.get("key")
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                if isinstance(sub_value, str) and "impediment" in sub_value.lower():
                    logger.debug(
                        "Issue %s flagged as impediment via %s=%s",
                        key,
                        sub_key,
                        sub_value,
                    )
                    return True
                if isinstance(sub_value, (dict, list)) and self._custom_field_contains_impediment(issue, sub_value):
                    return True
        elif isinstance(value, list):
            for entry in value:
                if self._custom_field_contains_impediment(issue, entry):
                    return True
        elif isinstance(value, str) and "impediment" in value.lower():
            logger.debug(
                "Issue %s flagged as impediment via custom field string",
                key,
            )
            return True
        return False


class _HTMLStructureValidator(HTMLParser):
    VOID_ELEMENTS = {
        "area",
        "base",
        "br",
        "col",
        "embed",
        "hr",
        "img",
        "input",
        "link",
        "meta",
        "param",
        "source",
        "track",
        "wbr",
    }

    def __init__(self) -> None:
        super().__init__()
        self.stack: List[str] = []
        self.errors: List[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:  # pragma: no cover - parsing
        tag_lower = tag.lower()
        if tag_lower in self.VOID_ELEMENTS:
            return
        self.stack.append(tag_lower)

    def handle_startendtag(self, tag: str, attrs) -> None:  # pragma: no cover - parsing
        return

    def handle_endtag(self, tag: str) -> None:  # pragma: no cover - parsing
        tag_lower = tag.lower()
        if tag_lower in self.VOID_ELEMENTS:
            return
        if not self.stack:
            self.errors.append(f"Unexpected closing tag </{tag}>")
            return
        expected = self.stack.pop()
        if expected != tag_lower:
            self.errors.append(
                f"Mismatched closing tag </{tag}> expected </{expected}>"
            )

    def close(self) -> None:  # pragma: no cover - parsing
        super().close()
        while self.stack:
            leftover = self.stack.pop()
            self.errors.append(f"Unclosed tag <{leftover}>")

    def _hydrate_issue(self, issue_key: str) -> dict:
        fields = [
            "summary",
            "description",
            "status",
            "assignee",
            "reporter",
            "priority",
            "labels",
            "comment",
            "created",
            "updated",
            "flagged",
            "customfield_16801",
        ]
        expanded = self.jira_client.get_issue(
            issue_key,
            fields=fields,
            expand=["changelog"],
        )
        return expanded

    def _custom_field_contains_impediment(self, issue: dict, value) -> bool:
        key = issue.get("key")
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                if isinstance(sub_value, str) and "impediment" in sub_value.lower():
                    logger.debug(
                        "Issue %s flagged as impediment via %s=%s",
                        key,
                        sub_key,
                        sub_value,
                    )
                    return True
                if isinstance(sub_value, (dict, list)) and self._custom_field_contains_impediment(issue, sub_value):
                    return True
        elif isinstance(value, list):
            for entry in value:
                if self._custom_field_contains_impediment(issue, entry):
                    return True
        elif isinstance(value, str) and "impediment" in value.lower():
            logger.debug(
                "Issue %s flagged as impediment via custom field string",
                key,
            )
            return True
        return False
