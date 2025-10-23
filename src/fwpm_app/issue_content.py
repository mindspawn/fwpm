from __future__ import annotations

import re
import logging
from datetime import datetime
from typing import Dict, List, Protocol

from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from .defaults import IGNORE_COMMENTS_FROM

_IGNORE_COMMENTS_NORMALIZED = {value.lower() for value in IGNORE_COMMENTS_FROM}


class IssueContentProvider(Protocol):
    def build_issue_text(self, issue: Dict) -> str:
        ...


class DefaultIssueContentProvider:
    """Builds a readable text block from core JIRA fields."""

    def build_issue_text(self, issue: Dict) -> str:
        self._build_display_name_cache(issue)
        fields = issue.get("fields", {})
        summary = fields.get("summary", "")
        description = self._clean_html(fields.get("description"))
        status = (fields.get("status") or {}).get("name", "Unknown")
        assignee = (fields.get("assignee") or {}).get("displayName", "Unassigned")
        reporter = (fields.get("reporter") or {}).get("displayName", "Unknown")
        created = self._format_timestamp(fields.get("created"))
        updated = self._format_timestamp(fields.get("updated"))

        comments = self._extract_comments(fields, issue.get("key"))

        parts = [
            f"Issue Key: {issue.get('key')}",
            f"Summary: {summary}",
            f"Status: {status}",
            f"Assignee: {assignee}",
            f"Reporter: {reporter}",
            f"Created: {created}",
            f"Updated: {updated}",
            "",
            "Description:",
            description or "<no description>",
        ]

        if comments:
            parts.append("")
            parts.append("Comments:")
            parts.extend(comments)

        return "\n".join(part for part in parts if part is not None)

    def _extract_comments(self, fields: Dict, issue_key: str | None) -> List[str]:
        comment_field = fields.get("comment") or {}
        comment_data = comment_field.get("comments", [])
        logger = logging.getLogger(__name__)
        logger.debug(
            "Issue %s comments fetched: %s/%s",
            issue_key,
            len(comment_data),
            comment_field.get("total"),
        )
        formatted = []
        for comment in comment_data:
            if self._should_ignore_comment(comment):
                continue
            author = (comment.get("author") or {}).get("displayName", "Unknown")
            body = self._extract_comment_body(comment)
            timestamp = self._format_timestamp(comment.get("created"))
            formatted.append(
                f"- {timestamp} – {author}: {body or 'empty comment'}"
            )
        return formatted

    def _clean_html(self, value) -> str:
        if not value:
            return ""
        if not isinstance(value, str):
            value = str(value)
        soup = BeautifulSoup(value, "html.parser")
        text = soup.get_text("\n", strip=True)
        text = self._replace_mentions(text)
        return text.replace("\r", "")

    def _format_timestamp(self, value: str | None) -> str:
        if not value:
            return "Unknown time"
        formats = ["%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"]
        parsed = None
        for fmt in formats:
            try:
                parsed = datetime.strptime(value, fmt)
                break
            except ValueError:
                continue
        if not parsed:
            return value
        pst = parsed.astimezone(ZoneInfo("America/Los_Angeles"))
        return pst.strftime("%Y-%m-%d %H:%M %Z")

    def _should_ignore_comment(self, comment: Dict) -> bool:
        if not _IGNORE_COMMENTS_NORMALIZED:
            return False
        author = comment.get("author") or {}
        identifiers = [
            author.get("accountId"),
            author.get("name"),
            author.get("key"),
            author.get("emailAddress"),
        ]
        identifiers = [value for value in identifiers if value]
        for identifier in identifiers:
            if isinstance(identifier, str) and identifier.lower() in _IGNORE_COMMENTS_NORMALIZED:
                return True
        return False

    def _build_display_name_cache(self, issue: Dict) -> None:
        fields = issue.get("fields") or {}
        author_fields = [
            fields.get("assignee"),
            fields.get("reporter"),
        ]
        comments = (fields.get("comment") or {}).get("comments", [])
        for comment in comments:
            author_fields.append(comment.get("author"))

        cache = getattr(self, "_mention_cache", None)
        if cache is None:
            cache = {}
            setattr(self, "_mention_cache", cache)

        for author in author_fields:
            if not isinstance(author, dict):
                continue
            display_name = author.get("displayName")
            if not display_name:
                continue
            for key_field in ["accountId", "name", "key", "emailAddress"]:
                identifier = author.get(key_field)
                if isinstance(identifier, str) and identifier:
                    cache[identifier.lower()] = display_name

    def _replace_mentions(self, text: str) -> str:
        cache = getattr(self, "_mention_cache", None) or {}

        def repl(match: re.Match) -> str:
            identifier = match.group("identifier")
            if not identifier:
                return match.group(0)
            display = cache.get(identifier.lower())
            return display or identifier

        pattern = re.compile(r"\[~(?:accountid:)?(?P<identifier>[\w@\.\-]+)\]")
        return pattern.sub(repl, text)

    def _extract_comment_body(self, comment: Dict) -> str:
        body = comment.get("body")
        if isinstance(body, str):
            cleaned = self._clean_html(body)
            if cleaned:
                return cleaned
        if isinstance(body, dict):
            rendered = self._extract_adf_text(body)
            if rendered:
                return rendered
        rendered_body = comment.get("renderedBody")
        if isinstance(rendered_body, str):
            rendered = self._clean_html(rendered_body)
            if rendered:
                return rendered
        return self._clean_html(body)

    def _extract_adf_text(self, node: Dict) -> str:
        # Atlassian Document Format traversal (best-effort plain text)
        parts: List[str] = []

        def walk(element, parent_type: str = "") -> None:
            if isinstance(element, dict):
                elem_type = element.get("type")
                if elem_type == "text":
                    text = element.get("text", "")
                    if text:
                        parts.append(text)
                else:
                    for child in element.get("content", []) or []:
                        walk(child, elem_type)
                    if elem_type in {"paragraph", "heading", "listItem"}:
                        parts.append("\n")
            elif isinstance(element, list):
                for child in element:
                    walk(child, parent_type)

        walk(node)
        joined = "".join(parts).strip()
        return self._clean_html(joined) if joined else ""
