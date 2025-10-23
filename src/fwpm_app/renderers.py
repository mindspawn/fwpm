from __future__ import annotations

import html
from datetime import datetime, timezone
from typing import Iterable, Tuple

import markdown
from urllib.parse import quote_plus

from .defaults import INFO_HEADER

def build_confluence_storage(
    jira_base_url: str,
    filter_id: str,
    filter_name: str,
    total_issues: int,
    issue_blocks: Iterable[
        Tuple[str, str, str, str | None, str, str, Tuple[str, ...], str, bool, str]
    ],
) -> str:
    """
    Build Confluence storage-format HTML with sections per issue.

    Args:
        jira_base_url: Base URL for linking to issues.
        filter_id: The JIRA filter identifier used.
        filter_name: The JIRA filter name.
        total_issues: Count of issues returned by the filter.
        issue_blocks: Iterable of tuples `(issue_key, issue_summary, assignee_name, assignee_url,
        reporter_name, priority_name, labels, status, is_impediment, generated_text)`.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    filter_url = f"{jira_base_url.rstrip('/')}/issues/?filter={quote_plus(filter_id)}"
    safe_filter_id = html.escape(filter_id)
    safe_filter_name = html.escape(filter_name or "")
    filter_name_fragment = f" ({safe_filter_name})" if safe_filter_name else ""
    toc_macro = (
        '<ac:structured-macro ac:name="toc">'
        '<ac:parameter ac:name="maxLevel">1</ac:parameter>'
        "<ac:rich-text-body/>"
        "</ac:structured-macro>"
    )
    info_panel = _build_info_panel(INFO_HEADER)
    info_section = "".join(
        [
            "<h1>Info</h1>",
            info_panel,
            f"<p><strong>Generated:</strong> {html.escape(timestamp)} UTC</p>",
            (
                f"<p><strong>Filter:</strong> <a href=\"{filter_url}\">{safe_filter_id}</a>"
                f"{filter_name_fragment}</p>"
            ),
            f"<p><strong>Total issues:</strong> {total_issues}</p>",
            "<p>Review all generated notes for accuracy before wider sharing.</p>",
        ]
    )

    sections = []
    for (
        issue_key,
        summary,
        assignee_name,
        assignee_url,
        reporter_name,
        priority_name,
        labels,
        status,
        is_impediment,
        llm_text,
    ) in issue_blocks:
        url = f"{jira_base_url.rstrip('/')}/browse/{issue_key}"
        safe_key = html.escape(issue_key)
        safe_summary = html.escape(summary or "")
        safe_status = html.escape(status or "Unknown")
        safe_assignee_name = html.escape(assignee_name or "Unassigned")
        assignee_html = safe_assignee_name
        if assignee_url:
            assignee_html = f"<a href=\"{html.escape(assignee_url)}\">{safe_assignee_name}</a>"
        reporter_html = html.escape(reporter_name or "Unknown")
        priority_html = html.escape(priority_name or "None")
        labels_html = ", ".join(html.escape(label) for label in labels) if labels else "None"
        issue_heading = (
            f"<h1><a href=\"{html.escape(url)}\">{safe_key}</a>: {safe_summary}"
            f" ({safe_status})</h1>"
        )
        flag_html = _impediment_badge() if is_impediment else ""
        assignee_line = (
            "<p>"
            f"{flag_html}"
            f"<strong>Assignee:</strong> {assignee_html} | "
            f"<strong>Reporter:</strong> {reporter_html} | "
            f"<strong>Priority:</strong> {priority_html} | "
            f"<strong>Labels:</strong> {labels_html}"
            "</p>"
        )
        safe_body = _render_markdown(llm_text)
        llm_section = f"<p><strong>Generated Notes:</strong></p>{safe_body}"

        section = "".join([issue_heading, assignee_line, llm_section])
        sections.append(section)

    return toc_macro + info_section + "".join(sections)


def _render_markdown(text: str) -> str:
    converted = markdown.markdown(text or "", extensions=[])
    return converted


def _build_info_panel(text: str) -> str:
    if not text:
        return ""
    escaped_text = html.escape(text)
    return (
        '<ac:structured-macro ac:name="info">'
        "<ac:parameter ac:name=\"icon\">information</ac:parameter>"
        "<ac:rich-text-body>"
        f"<p>{escaped_text}</p>"
        "</ac:rich-text-body>"
        "</ac:structured-macro>"
    )


def _impediment_badge() -> str:
    return (
        '<ac:structured-macro ac:name="status">'
        '<ac:parameter ac:name="colour">red</ac:parameter>'
        '<ac:parameter ac:name="title">IMPEDIMENT</ac:parameter>'
        '<ac:parameter ac:name="subtle">false</ac:parameter>'
        "</ac:structured-macro> "
    )
