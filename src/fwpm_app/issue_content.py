from __future__ import annotations

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
        fields = issue.get("fields", {})
        summary = fields.get("summary", "")
        description = self._clean_html(fields.get("description"))
        status = (fields.get("status") or {}).get("name", "Unknown")
        assignee = (fields.get("assignee") or {}).get("displayName", "Unassigned")
        reporter = (fields.get("reporter") or {}).get("displayName", "Unknown")
        created = self._format_timestamp(fields.get("created"))
        updated = self._format_timestamp(fields.get("updated"))

        comments = self._extract_comments(fields)

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

    def _extract_comments(self, fields: Dict) -> List[str]:
        comment_data = (fields.get("comment") or {}).get("comments", [])
        formatted = []
        for comment in comment_data:
            if self._should_ignore_comment(comment):
                continue
            author = (comment.get("author") or {}).get("displayName", "Unknown")
            body = self._clean_html(comment.get("body"))
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
