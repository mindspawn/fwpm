from __future__ import annotations

from typing import Dict, List, Protocol

from bs4 import BeautifulSoup


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
            formatted.append(f"- {author}: {body or 'empty comment'}")
        return formatted

    def _clean_html(self, value) -> str:
        if not value:
            return ""
        if not isinstance(value, str):
            value = str(value)
        soup = BeautifulSoup(value, "html.parser")
        text = soup.get_text("\n", strip=True)
        return text
