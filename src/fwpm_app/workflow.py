from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
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
    IGNORE_COMMENTS_FROM,
)
from bs4 import BeautifulSoup
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
            "components",
            "created",
            "updated",
            "flagged",
            "customfield_10719",
            "customfield_23301",
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
            recent_comments = self._collect_recent_comments(hydrated_issue)
            if not recent_comments:
                placeholder_outputs.append((hydrated_issue, self._no_recent_activity_message()))
                continue
            background_text = self._build_background_text(hydrated_issue)
            recent_text = self._format_comment_entries(recent_comments)
            user_prompt = self._build_user_prompt(background_text, recent_text)
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
            recent_comments = self._collect_recent_comments(hydrated_issue)

            if not recent_comments:
                message = self._no_recent_activity_message()
                outputs.append((hydrated_issue, message))
                logger.info(
                    "Skipping LLM for %s; no comment activity in the last %s hours",
                    hydrated_issue.get("key"),
                    self.app_config.comment_lookback_hours,
                )
                continue

            background_text = self._build_background_text(hydrated_issue)
            recent_comments_text = self._format_comment_entries(recent_comments)

            user_prompt = self._build_user_prompt(background_text, recent_comments_text)
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
            response_text = self._strip_think_blocks(response_text)
            response_text = self._demote_markdown_headings(response_text)
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
                self._components(issue),
                self._status_name(issue),
                self._is_impediment(issue),
                self._product_names(issue),
                self._customer_names(issue),
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

    def _build_user_prompt(self, background_text: str, recent_comments_text: str) -> str:
        now_pacific = datetime.now(ZoneInfo("America/Los_Angeles"))
        timestamp = now_pacific.strftime("%Y-%m-%d %H:%M %Z")

        context_sections = [
            # "Background:",
            background_text.strip(),
            "",
            f"Recent JIRA task comment activity (last {self.app_config.comment_lookback_hours} hours):",
            recent_comments_text.strip(),
        ]
        context = "\n".join(section for section in context_sections if section)

        query_line = (
            f"{self.app_config.llm_user_prompt.strip()} Focus only on the recent comment activity "
            f"from the last {self.app_config.comment_lookback_hours} hours."
        )

        parts = [
            "Use the following context as your learned knowledge, inside <context></context> XML tags.",
            "<context>",
            context,
            "</context>",
            f"The current date and time is {timestamp}.",
            "When answering the user: If you don't know, just say you don't know.",
            "Avoid mentioning that you obtained the information from the context.",
            "Answer according to the language of the user's question.",
            "Maintain a professional tone",
            "Do not generate unnecessary text such as - \"Here's a summary\"",
            "Base your answer solely on the given context. Do not infer, assume, or fabricate information.",
            "Given the context information, answer the query.",
            "Query:",
            "Create an executive summary of all the above JIRA comments. Bullets are preferred for the summary."
        ]
        return "\n\n".join(part for part in parts if part)

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

    def _collect_recent_comments(
        self, issue: dict
    ) -> List[Tuple[dict, datetime]]:
        fields = issue.get("fields") or {}
        comment_field = fields.get("comment") or {}
        comments = comment_field.get("comments", []) or []
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self.app_config.comment_lookback_hours)
        recent: List[Tuple[dict, datetime]] = []
        normalized_ignore = {value.lower() for value in IGNORE_COMMENTS_FROM}
        for comment in comments:
            author_info = comment.get("author") or {}
            identifiers = [
                author_info.get("accountId"),
                author_info.get("name"),
                author_info.get("key"),
                author_info.get("emailAddress"),
            ]
            if any(
                isinstance(identifier, str) and identifier.lower() in normalized_ignore
                for identifier in identifiers
                if identifier
            ):
                continue
            created_raw = comment.get("created")
            created_dt = self._parse_comment_datetime(created_raw)
            if created_dt is None:
                continue
            entry = (comment, created_dt)
            if created_dt >= cutoff:
                recent.append(entry)
        return recent

    def _format_comment_entries(self, entries: List[Tuple[dict, datetime]]) -> str:
        if not entries:
            return ""
        pacific = ZoneInfo("America/Los_Angeles")
        formatted: List[str] = []
        for comment, created in entries:
            author = ((comment.get("author") or {}).get("displayName")) or "Unknown"
            created_local = created.astimezone(pacific).strftime("%Y-%m-%d %H:%M %Z")
            text = self._comment_text(comment)
            if not text:
                text = "<no content>"
            cleaned = text.replace("\r\n", "\n").replace("\r", "\n").strip()
            formatted.append(f"[COMMENT at {created_local} from {author}]\n{cleaned}")
        return "\n\n".join(formatted)

    def _build_background_text(
        self, issue: dict
    ) -> str:
        fields = issue.get("fields") or {}
        summary = fields.get("summary") or ""
        lines: List[str] = [] # [f"Issue: {issue.get('key')} – {summary}"]

        if self.app_config.include_description_background:
            description = fields.get("description")
            desc_text = self._clean_text(description)
            if desc_text:
                lines.append("Description:\n" + desc_text)

        return "\n\n".join(lines)

    def _no_recent_activity_message(self) -> str:
        hours = self.app_config.comment_lookback_hours
        return f"<p><em>No comment activity in the last {hours} hours.</em></p>"

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

    def _strip_think_blocks(self, text: str) -> str:
        if not text:
            return text
        return re.sub(r"<think>[\s\S]*?</think>", "", text, flags=re.IGNORECASE)

    def _demote_markdown_headings(self, text: str) -> str:
        if not text:
            return text
        return re.sub(
            r"^(#{1,6})\s*(.+)$",
            lambda m: f"**{m.group(2).strip()}**",
            text,
            flags=re.MULTILINE,
        )

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

    def _components(self, issue: dict) -> Tuple[str, ...]:
        value = (issue.get("fields") or {}).get("components") or []
        values = self._extract_field_values(value)
        return tuple(values)

    def _product_names(self, issue: dict) -> str:
        value = (issue.get("fields") or {}).get("customfield_10719")
        values = self._extract_field_values(value)
        return ", ".join(values) if values else "Unknown"

    def _customer_names(self, issue: dict) -> str:
        value = (issue.get("fields") or {}).get("customfield_23301")
        values = self._extract_field_values(value)
        return ", ".join(values) if values else "Unknown"

    def _status_name(self, issue: dict) -> str:
        status = (issue.get("fields") or {}).get("status") or {}
        return status.get("name", "Unknown")

    def _comment_text(self, comment: dict) -> str:
        rendered = comment.get("renderedBody")
        if rendered:
            return self._clean_text(rendered)
        body = comment.get("body")
        if isinstance(body, str):
            return self._clean_text(body)
        if isinstance(body, dict):
            return self._clean_text(self._extract_adf_text(body))
        return ""

    def _clean_text(self, value) -> str:
        if not value:
            return ""
        if not isinstance(value, str):
            value = str(value)
        text = BeautifulSoup(value, "html.parser").get_text("\n", strip=True)
        text = re.sub(r"\[~(?:accountid:)?([^\]]+)\]", r"\1", text)
        return text.strip()

    def _extract_adf_text(self, node) -> str:
        parts: List[str] = []

        def walk(elem) -> None:
            if isinstance(elem, dict):
                elem_type = elem.get("type")
                if elem_type == "text":
                    text = elem.get("text", "")
                    if text:
                        parts.append(text)
                elif elem_type == "hardBreak":
                    parts.append("\n")
                else:
                    for child in elem.get("content", []) or []:
                        walk(child)
                    if elem_type in {"paragraph", "heading", "listItem"}:
                        parts.append("\n")
            elif isinstance(elem, list):
                for child in elem:
                    walk(child)

        walk(node)
        return "".join(parts).strip()

    def _parse_comment_datetime(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        logger.debug("Unable to parse comment timestamp: %s", value)
        return None

    def _extract_field_values(self, value) -> List[str]:
        results: List[str] = []
        if value is None:
            return results
        if isinstance(value, str):
            if value.strip():
                results.append(value.strip())
            return results
        if isinstance(value, dict):
            for key in ("value", "name", "displayName", "title"):
                field_val = value.get(key)
                if isinstance(field_val, str) and field_val.strip():
                    results.append(field_val.strip())
            if "children" in value and isinstance(value["children"], list):
                for child in value["children"]:
                    results.extend(self._extract_field_values(child))
            return results
        if isinstance(value, list):
            for item in value:
                results.extend(self._extract_field_values(item))
        return [v for v in results if v]

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
            "customfield_10719",
            "customfield_23301",
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
