from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Protocol

from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo


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

        comments = self._extract_comments(fields)

        parts = [
            f"Issue Key: {issue.get('key')}",
            f"Summary: {summary}",
            f"Status: {status}",
            f"Assignee: {assignee}",
            f"Reporter: {reporter}",
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
