from __future__ import annotations

import html
from datetime import datetime, timezone
from typing import Iterable, Tuple

import markdown
from urllib.parse import quote_plus


def build_confluence_storage(
    jira_base_url: str,
    filter_id: str,
    filter_name: str,
    total_issues: int,
    issue_blocks: Iterable[Tuple[str, str, str, str | None, str]],
) -> str:
    """
    Build Confluence storage-format HTML with sections per issue.

    Args:
        jira_base_url: Base URL for linking to issues.
        filter_id: The JIRA filter identifier used.
        filter_name: The JIRA filter name.
        total_issues: Count of issues returned by the filter.
        issue_blocks: Iterable of tuples `(issue_key, issue_summary, assignee_name, assignee_url, generated_text)`.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    filter_url = f"{jira_base_url.rstrip('/')}/issues/?filter={quote_plus(filter_id)}"
    safe_filter_id = html.escape(filter_id)
    safe_filter_name = html.escape(filter_name or "")
    filter_name_fragment = f" ({safe_filter_name})" if safe_filter_name else ""
    toc_macro = (
        '<ac:structured-macro ac:name="toc">'
        "<ac:rich-text-body/>"
        "</ac:structured-macro>"
    )
    info_section = "".join(
        [
            "<h1>Info</h1>",
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
    for issue_key, summary, assignee_name, assignee_url, llm_text in issue_blocks:
        url = f"{jira_base_url.rstrip('/')}/browse/{issue_key}"
        safe_key = html.escape(issue_key)
        safe_summary = html.escape(summary or "")
        safe_assignee_name = html.escape(assignee_name or "Unassigned")
        assignee_html = safe_assignee_name
        if assignee_url:
            assignee_html = f"<a href=\"{html.escape(assignee_url)}\">{safe_assignee_name}</a>"
        issue_heading = f"<h1><a href=\"{html.escape(url)}\">{safe_key}</a>: {safe_summary}</h1>"
        assignee_line = f"<p><strong>Assignee:</strong> {assignee_html}</p>"
        safe_body = _render_markdown(llm_text)
        llm_section = f"<p><strong>Generated Notes:</strong></p>{safe_body}"

        section = "".join([issue_heading, assignee_line, llm_section])
        sections.append(section)

    return toc_macro + info_section + "".join(sections)


def _render_markdown(text: str) -> str:
    converted = markdown.markdown(text or "", extensions=[])
    return converted
