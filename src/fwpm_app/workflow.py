from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Tuple, Optional
import smtplib
from email.message import EmailMessage
import re
import unicodedata
from html.parser import HTMLParser
from urllib.parse import quote_plus
import html

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
from bs4 import BeautifulSoup, Tag, NavigableString
from .issue_content import DefaultIssueContentProvider, IssueContentProvider
from .jira_client import JiraClient
from .llm_client import LLMClient
from .renderers import build_confluence_storage

EMAIL_INLINE_CSS = """
<style>
body { font-family: Arial, Helvetica, sans-serif; font-size: 14px; color: #172B4D; }
table { border-collapse: collapse; }
.toc-indentation { margin-left: 16px; }
.status-macro, .aui-lozenge {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 3px;
  font-size: 12px;
  font-weight: 600;
  color: #fff;
  text-transform: none;
  margin-right: 4px;
}
.aui-lozenge-subtle {
  border: 1px solid transparent;
  background-color: rgba(9, 30, 66, 0.25);
  color: #172B4D;
}
.aui-lozenge-success { background-color: #57d9a3 !important; }
.aui-lozenge-complete { background-color: #4c9aff !important; }
.aui-lozenge-current { background-color: #ffab00 !important; }
.aui-lozenge-moved { background-color: #6554C0 !important; }
.aui-lozenge-error, .aui-lozenge-removed { background-color: #ff5630 !important; }
.aui-lozenge { background-color: #7A869A; color: #fff !important; }
.status-macro { color: #fff !important; }
.toc-macro { margin-bottom: 16px; }
</style>
"""

STATUS_NAME_HEX = {
    "red": "#FF5630",
    "yellow": "#FFAB00",
    "green": "#36B37E",
    "blue": "#4C9AFF",
    "grey": "#6B778C",
    "gray": "#6B778C",
    "purple": "#6554C0",
    "teal": "#00B8D9",
    "lime": "#A5D753",
    "brown": "#8C6E64",
}

STATUS_CLASS_HEX = {
    "aui-lozenge-success": "#57D9A3",
    "aui-lozenge-complete": "#4C9AFF",
    "aui-lozenge-current": "#FFAB00",
    "aui-lozenge-moved": "#6554C0",
    "aui-lozenge-error": "#FF5630",
    "aui-lozenge-removed": "#FF5630",
    "aui-lozenge-default": "#7A869A",
    "aui-lozenge": "#7A869A",
}

DEFAULT_STATUS_HEX = "#7A869A"
SUBTLE_BACKGROUND_HEX = "#DFE1E6"
SUBTLE_BORDER_HEX = "#A5ADBA"
DEFAULT_TEXT_COLOR = "#172B4D"
INFO_PANEL_BACKGROUND = "#E9F2FF"
PANEL_DEFAULT_BORDER = "#0052CC"

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
        result = self._publish_confluence_page(filter_cfg, body)
        self._send_email_if_enabled(filter_cfg, result, body)
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
        placeholder_outputs: List[Tuple[dict, str, bool]] = []
        for issue in issues:
            hydrated_issue = self._hydrate_issue(issue["key"])
            recent_comments = self._collect_recent_comments(hydrated_issue)
            if not recent_comments:
                placeholder_outputs.append(
                    (
                        hydrated_issue,
                        self._no_recent_activity_message(),
                        False,
                    )
                )
                continue
            background_text = self._build_background_text(hydrated_issue)
            recent_text = self._format_comment_entries(recent_comments)
            user_prompt = self._build_user_prompt(background_text, recent_text)
            self._persist_prompt(hydrated_issue.get("key"), user_prompt)
            placeholder_outputs.append(
                (
                    hydrated_issue,
                    "This is where the LLM response is",
                    True,
                )
            )
        body = self._build_confluence_body(filter_id, filter_details, placeholder_outputs, filter_cfg)
        self._persist_confluence_body(body)
        if self.validate_html:
            self._validate_html(body)
        result = self._publish_confluence_page(filter_cfg, body)
        self._send_email_if_enabled(filter_cfg, result, body)
        logger.info(
            "Placeholder workflow completed for filter %s in %.2f seconds",
            filter_id,
            time.time() - workflow_start,
        )

    def _publish_confluence_page(
        self,
        filter_cfg: FilterConfig,
        body: str,
    ) -> dict:
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
        return result

    def _send_email_if_enabled(self, filter_cfg: FilterConfig, page_result: dict, storage_body: str) -> None:
        if not self.app_config.email_enabled:
            return
        recipients = [r.strip() for r in filter_cfg.email_recipients if r.strip()]
        if not recipients:
            logger.debug("Email sending enabled but no recipients provided; skipping.")
            return
        smtp_host = self.app_config.email_smtp_host
        if not smtp_host:
            logger.warning("Email sending enabled but EMAIL_SMTP_HOST not configured; skipping.")
            return

        page_id = page_result.get("id")
        if not page_id:
            logger.warning("Cannot send email: Confluence page id missing in response.")
            return

        try:
            page_view = self.confluence_client.get_page_export_view(page_id)
            rendered_html = (
                (((page_view.get("body") or {}).get("export_view") or {}).get("value"))
                or storage_body
            )
        except Exception as exc:  # pragma: no cover - network failures
            logger.warning("Failed to fetch rendered Confluence HTML: %s", exc)
            rendered_html = storage_body

        links = page_result.get("_links", {}) or {}
        base_link = links.get("base")
        webui = links.get("webui")
        if base_link and webui:
            page_url = f"{base_link}{webui}"
        else:
            base = self.app_config.confluence_base_url.rstrip("/")
            page_url = f"{base}{webui or ''}"

        base_href = None
        if base_link:
            base_href = base_link.rstrip("/") + "/"
        base_tag = f"<base href=\"{html.escape(base_href)}\" />" if base_href else ""

        rendered_html = self._enhance_email_html(rendered_html, storage_body)

        html_message = (
            "<html><head>"
            f"{base_tag}{EMAIL_INLINE_CSS}"
            "</head><body>"
            f"<p>The page has been published to Confluence. Version history can be viewed "
            f"<a href=\"{html.escape(page_url)}\">here</a>.</p>"
            f"{rendered_html}"
            "</body></html>"
        )
        text_message = (
            "The page has been published to Confluence. Version history can be viewed here: "
            f"{page_url}"
        )

        email_msg = EmailMessage()
        email_msg["Subject"] = filter_cfg.confluence.page_name
        email_msg["From"] = self.app_config.email_from
        email_msg["To"] = ", ".join(recipients)
        email_msg.set_content(text_message)
        email_msg.add_alternative(html_message, subtype="html")

        try:
            with smtplib.SMTP(self.app_config.email_smtp_host, 25, timeout=30) as smtp:
                smtp.send_message(email_msg)
            logger.info("Email sent to %s", recipients)
        except Exception as exc:  # pragma: no cover - network failures
            logger.warning("Failed to send email notification: %s", exc)

    def _run_llm_round(
        self, issues: List[dict], filter_cfg: FilterConfig
    ) -> List[Tuple[dict, str, bool]]:
        outputs: List[Tuple[dict, str, bool]] = []
        start = time.time()
        overall_start = start
        total = len(issues)

        for index, issue in enumerate(issues, start=1):
            hydrated_issue = self._hydrate_issue(issue["key"])
            recent_comments = self._collect_recent_comments(hydrated_issue)

            if not recent_comments:
                message = self._no_recent_activity_message()
                outputs.append((hydrated_issue, message, False))
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
            outputs.append((hydrated_issue, response_text, True))
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
        llm_outputs: List[Tuple[dict, str, bool]],
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
                should_panel,
            )
            for issue, response_text, should_panel in llm_outputs
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
            query_line,
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
        lines: List[str] = [f"Issue: {issue.get('key')} – {summary}"]

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

    def _enhance_email_html(self, rendered_html: str, storage_body: str) -> str:
        """
        Inline key Confluence macro styles so emails render without the official stylesheet.

        Args:
            rendered_html: HTML returned from the Confluence export view.
            storage_body: Storage-format HTML used to build the page (fallback).
        """
        candidate = rendered_html or storage_body or ""
        if not candidate:
            return ""
        soup = BeautifulSoup(candidate, "html.parser")

        # Convert storage-format macros if they are still present (export fallback).
        if soup.find("ac:structured-macro"):
            soup = self._materialize_structured_macros(soup)

        self._style_status_macros(soup)
        self._style_info_macros(soup)
        self._style_panel_macros(soup)
        self._strip_table_of_contents(soup)
        return str(soup)

    def _materialize_structured_macros(self, soup: BeautifulSoup) -> BeautifulSoup:
        for macro in list(soup.find_all("ac:structured-macro")):
            macro_name = macro.get("ac:name", "").lower()
            if macro_name == "status":
                replacement = self._build_status_span_from_macro(soup, macro)
                macro.replace_with(replacement)
            elif macro_name == "info":
                replacement = self._build_info_panel_from_macro(soup, macro)
                macro.replace_with(replacement)
            elif macro_name == "panel":
                replacement = self._build_panel_from_macro(soup, macro)
                macro.replace_with(replacement)
            else:
                macro.decompose()
        return soup

    def _build_status_span_from_macro(self, soup: BeautifulSoup, macro: Tag) -> Tag:
        params = {
            param.get("ac:name", "").lower(): (param.string or "").strip()
            for param in macro.find_all("ac:parameter")
        }
        title = params.get("title") or (macro.get_text() or "").strip() or "Status"
        colour = params.get("colour") or params.get("color") or ""
        subtle = params.get("subtle", "").lower() == "true"
        span = soup.new_tag("span")
        span.string = title
        self._apply_status_styles(span, colour, subtle)
        return span

    def _build_info_panel_from_macro(self, soup: BeautifulSoup, macro: Tag) -> Tag:
        params = {
            param.get("ac:name", "").lower(): (param.string or "").strip()
            for param in macro.find_all("ac:parameter")
        }
        icon = params.get("icon", "information").capitalize()
        title_param = params.get("title", "") or ""
        title_text = title_param.strip() or icon
        include_heading = bool(title_param.strip() and title_param.strip().lower() != "info")
        body = macro.find("ac:rich-text-body")
        return self._build_panel_element(
            soup=soup,
            title_text=title_text,
            body_node=body,
            original_panel=None,
            include_heading=include_heading,
            border_color=PANEL_DEFAULT_BORDER,
            background_color=INFO_PANEL_BACKGROUND,
        )

    def _build_panel_from_macro(self, soup: BeautifulSoup, macro: Tag) -> Tag:
        params = {
            param.get("ac:name", "").lower(): (param.string or "").strip()
            for param in macro.find_all("ac:parameter")
        }
        title_text = (params.get("title") or "").strip()
        include_heading = bool(title_text)
        border_color = params.get("bordercolor") or params.get("bordercolour") or PANEL_DEFAULT_BORDER
        background_color = (
            params.get("bgcolor")
            or params.get("background")
            or params.get("backgroundcolor")
            or INFO_PANEL_BACKGROUND
        )
        body = macro.find("ac:rich-text-body")
        return self._build_panel_element(
            soup=soup,
            title_text=title_text,
            body_node=body,
            original_panel=None,
            include_heading=include_heading,
            border_color=border_color,
            background_color=background_color,
        )

    def _style_status_macros(self, soup: BeautifulSoup) -> None:
        for status in soup.select(".status-macro, .aui-lozenge"):
            classes = status.get("class", [])
            subtle = "aui-lozenge-subtle" in classes if classes else False
            colour = self._pick_colour_from_element(status)
            self._apply_status_styles(status, colour, subtle)

    def _style_info_macros(self, soup: BeautifulSoup) -> None:
        panels = soup.select(".confluence-information-macro")
        for panel in panels:
            replacement = self._build_info_panel_from_export(soup, panel)
            if replacement is not None:
                panel.replace_with(replacement)

    def _style_panel_macros(self, soup: BeautifulSoup) -> None:
        for panel in soup.select("div.panel"):
            classes = panel.get("class", [])
            if classes and any(cls.startswith("confluence-information-macro") for cls in classes):
                continue
            replacement = self._build_panel_from_export(soup, panel)
            if replacement is not None:
                panel.replace_with(replacement)

    def _strip_table_of_contents(self, soup: BeautifulSoup) -> None:
        selectors = [
            ".toc-macro",
            ".tocMacro",
            ".toc-macro-section",
            ".toc-macro-list",
            ".toc-macro-heading",
        ]
        for selector in selectors:
            for node in list(soup.select(selector)):
                node.decompose()
    def _apply_status_styles(self, element: Tag, colour: str | None, subtle: bool) -> None:
        colour_hex = self._normalise_colour(colour) or DEFAULT_STATUS_HEX
        if subtle:
            bg = SUBTLE_BACKGROUND_HEX
            text_colour = DEFAULT_TEXT_COLOR
            border = SUBTLE_BORDER_HEX
        else:
            bg = colour_hex
            text_colour = self._status_text_colour(bg)
            border = None

        display_text = element.get_text(strip=True) or element.get("title") or "Status"
        safe_text = html.escape(display_text)
        border_style = f"border:1px solid {border};" if border else "border:0;"
        border_radius = "border-radius:3px;"
        table_style = (
            "border-collapse:separate; border-spacing:0; display:inline-table; "
            "margin-right:4px; vertical-align:middle;"
        )
        td_style = (
            f"padding:2px 8px; mso-padding-alt:0px 8px 0px 8px; {border_style} {border_radius} "
            f"background-color:{bg}; color:{text_colour}; font-size:12px; font-weight:600; "
            "text-transform:none; line-height:1.3; mso-line-height-rule:exactly;"
        )
        td_attrs = f'bgcolor="{bg}" style="{td_style}"'
        table_html = (
            f'<table role="presentation" cellspacing="0" cellpadding="0" '
            f'style="{table_style}"><tr><td {td_attrs}>{safe_text}</td></tr></table>'
        )
        replacement = BeautifulSoup(table_html, "html.parser")
        table_tag = replacement.find("table")
        if table_tag is not None:
            element.replace_with(table_tag)
        else:
            self._append_style(element, "display:inline-block;")

    def _pick_colour_from_element(self, element: Tag) -> str | None:
        for attr in ("data-color", "data-bgcolor", "data-background"):
            colour = element.get(attr)
            if colour:
                return colour
        class_names = element.get("class", [])
        for class_name in class_names:
            if class_name in STATUS_CLASS_HEX:
                return STATUS_CLASS_HEX[class_name]
        style_attr = element.get("style", "")
        match = re.search(r"background-color\s*:\s*([^;]+)", style_attr)
        if match:
            return match.group(1).strip()
        return None

    def _normalise_colour(self, value: str | None) -> str | None:
        if not value:
            return None
        colour = value.strip()
        if not colour:
            return None
        lower = colour.lower()
        if lower in STATUS_NAME_HEX:
            return STATUS_NAME_HEX[lower]
        if lower.startswith("#"):
            if len(lower) == 4:
                r, g, b = lower[1], lower[2], lower[3]
                return f"#{r}{r}{g}{g}{b}{b}".upper()
            if len(lower) == 7:
                return lower.upper()
        if lower.startswith("rgb"):
            parts = re.findall(r"\d+", lower)
            if len(parts) >= 3:
                r, g, b = (max(0, min(255, int(part))) for part in parts[:3])
                return f"#{r:02X}{g:02X}{b:02X}"
        return None

    def _status_text_colour(self, background_hex: str) -> str:
        hex_code = background_hex.lstrip("#")
        try:
            r, g, b = (
                int(hex_code[i : i + 2], 16)
                for i in (0, 2, 4)
            )
        except (ValueError, TypeError):
            return "#FFFFFF"
        brightness = (0.299 * r) + (0.587 * g) + (0.114 * b)
        return DEFAULT_TEXT_COLOR if brightness >= 170 else "#FFFFFF"

    def _append_style(self, element: Tag, styles: str) -> None:
        existing = element.get("style", "")
        existing = existing.strip()
        addition = styles.strip().rstrip(";")
        if not addition:
            return
        if existing:
            if not existing.endswith(";"):
                existing = existing + ";"
            element["style"] = f"{existing} {addition};"
        else:
            element["style"] = f"{addition};"

    def _set_style(self, element: Tag, styles: str) -> None:
        clean = styles.strip().rstrip(";")
        element["style"] = f"{clean};" if clean else ""

    def _build_info_panel_from_export(self, soup: BeautifulSoup, panel: Tag) -> Tag | None:
        body = panel.select_one(".confluence-information-macro-body") or panel.select_one(".panelContent")
        title_elem = panel.select_one(".confluence-information-macro-title") or panel.select_one(".title-text")
        data_title = panel.get("data-macro-title") or panel.get("data-title")
        title_text = ""
        include_heading = False
        if title_elem and title_elem.get_text(strip=True):
            title_text = title_elem.get_text(strip=True)
            include_heading = title_text.strip().lower() != "info"
        elif isinstance(data_title, str) and data_title.strip():
            title_text = data_title.strip()
            include_heading = title_text.strip().lower() != "info"
        else:
            data_name = panel.get("data-macro-name")
            if isinstance(data_name, str) and data_name.strip():
                title_text = data_name.strip().capitalize()
                include_heading = title_text.strip().lower() != "info"
        if not title_text:
            title_text = "Info"

        replacement = self._build_panel_element(
            soup=soup,
            title_text=title_text,
            body_node=body,
            original_panel=panel,
            include_heading=include_heading,
            border_color=PANEL_DEFAULT_BORDER,
            background_color=INFO_PANEL_BACKGROUND,
        )
        return replacement

    def _build_panel_from_export(self, soup: BeautifulSoup, panel: Tag) -> Tag | None:
        content = panel.select_one(".panelContent") or panel.select_one(".panel-body")
        header = panel.select_one(".panelHeader") or panel.select_one(".panel-heading")
        title_text = ""
        include_heading = False
        if header and header.get_text(strip=True):
            title_text = header.get_text(strip=True)
            include_heading = True

        border_color = (
            panel.get("data-bordercolor")
            or panel.get("data-borderColor")
            or self._extract_style_color(panel, "border-color")
            or PANEL_DEFAULT_BORDER
        )
        background_color = (
            panel.get("data-bgcolor")
            or (content.get("data-bgcolor") if content else None)
            or self._extract_style_color(panel, "background-color")
            or self._extract_style_color(content, "background-color")
            or INFO_PANEL_BACKGROUND
        )

        return self._build_panel_element(
            soup=soup,
            title_text=title_text,
            body_node=content,
            original_panel=panel if content is None else None,
            include_heading=include_heading,
            border_color=border_color,
            background_color=background_color,
        )

    def _build_panel_element(
        self,
        soup: BeautifulSoup,
        title_text: str,
        body_node: Tag | None,
        original_panel: Tag | None = None,
        include_heading: bool = False,
        border_color: str | None = None,
        background_color: str | None = None,
    ) -> Tag:
        border = self._normalise_colour(border_color) or PANEL_DEFAULT_BORDER
        background = self._normalise_colour(background_color) or INFO_PANEL_BACKGROUND

        panel = soup.new_tag("div")
        self._set_style(panel, self._panel_container_style(border, background))

        inner = soup.new_tag("div")
        self._append_style(
            inner,
            "margin:0; padding:14px 18px; border:0; width:100%; "
            f"background-color:{background}; color:{DEFAULT_TEXT_COLOR}; line-height:1.3;",
        )

        if include_heading and title_text:
            heading = soup.new_tag("div")
            self._append_style(
                heading,
                f"font-weight:600; margin:0 0 8px 0; background-color:{background};",
            )
            heading.string = title_text
            inner.append(heading)

        content = soup.new_tag("div")
        self._append_style(
            content,
            f"margin:0; padding:0; border:0; width:100%; background-color:{background}; "
            f"color:{DEFAULT_TEXT_COLOR}; line-height:1.3;",
        )

        if body_node is not None:
            for child in list(body_node.contents):
                extracted = child.extract()
                if isinstance(extracted, Tag):
                    self._normalize_panel_child(extracted, background)
                content.append(extracted)
        elif original_panel is not None:
            for child in list(original_panel.contents):
                if isinstance(child, NavigableString):
                    if not child.strip():
                        continue
                    content.append(soup.new_string(child))
                    continue
                if not isinstance(child, Tag):
                    continue
                class_list = child.get("class", [])
                if class_list and any(
                    cls.startswith("confluence-information-macro") or cls.startswith("aui-icon")
                    for cls in class_list
                ):
                    continue
                extracted = child.extract()
                if isinstance(extracted, Tag):
                    self._normalize_panel_child(extracted, background)
                content.append(extracted)

        if not content.contents:
            content.append(soup.new_string(""))

        inner.append(content)
        panel.append(inner)
        return panel

    def _panel_container_style(self, border_color: str, background_color: str) -> str:
        return (
            f"margin:16px 0; border:1.5px solid {border_color}; border-radius:3px; "
            f"background-color:{background_color}; padding:0; color:{DEFAULT_TEXT_COLOR}; "
            "box-sizing:border-box; overflow:hidden;"
        )

    def _normalize_panel_child(self, element: Tag, background_color: str) -> None:
        name = (element.name or "").lower() if element.name else ""
        if name in {"p", "div", "ul", "ol", "li", "table", "tbody", "tr", "td", "th", "pre"}:
            self._append_style(
                element,
                f"margin:0; background-color:{background_color}; color:{DEFAULT_TEXT_COLOR}; "
                "line-height:1.3;",
            )
            if name in {"ul", "ol"}:
                self._append_style(element, "padding-left:20px;")
        for child in element.children:
            if isinstance(child, Tag):
                self._normalize_panel_child(child, background_color)

    def _extract_style_color(self, element: Tag | None, property_name: str) -> str | None:
        if element is None:
            return None
        style_attr = element.get("style", "")
        if not style_attr:
            return None
        match = re.search(
            rf"{property_name}\s*:\s*([^;]+)",
            style_attr,
            flags=re.IGNORECASE,
        )
        if match:
            return match.group(1).strip()
        return None


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
