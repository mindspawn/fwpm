from __future__ import annotations

import html
from datetime import datetime
from typing import Iterable, Tuple

import markdown
from urllib.parse import quote_plus

from zoneinfo import ZoneInfo

from .defaults import INFO_HEADER, LABEL_STATUS_MAP
from bs4 import BeautifulSoup

_DONE_STATUS_NAMES = {"done", "closed", "resolved", "cancelled"}

def build_confluence_storage(
    jira_base_url: str,
    filter_id: str,
    filter_name: str,
    total_issues: int,
    issue_blocks: Iterable[
        Tuple[
            str,
            str,
            str,
            str | None,
            str,
            str,
            Tuple[str, ...],
            Tuple[str, ...],
            str,
            bool,
            str,
            str,
            str,
            bool,
        ]
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
        reporter_name, priority_name, labels, components, status, is_impediment,
        product, customer, generated_text, should_panel)`.
    """
    timestamp = datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y-%m-%d %H:%M %Z")
    filter_url = f"{jira_base_url.rstrip('/')}/issues/?filter={quote_plus(filter_id)}"
    safe_filter_id = html.escape(filter_id)
    safe_filter_name = html.escape(filter_name or "")
    filter_name_fragment = f" ({safe_filter_name})" if safe_filter_name else ""
    toc_macro = (
        '<ac:structured-macro ac:name="toc">'
        '<ac:parameter ac:name="minLevel">3</ac:parameter>'
        '<ac:parameter ac:name="maxLevel">3</ac:parameter>'
        "<ac:rich-text-body/>"
        "</ac:structured-macro>"
    )
    panel_body = _assemble_info_panel_body(
        info_html=INFO_HEADER,
        generated_html=f"<strong>Generated:</strong> {html.escape(timestamp)}",
        filter_html=(
            f"<strong>Filter:</strong> <a href=\"{filter_url}\">{safe_filter_id}</a>{filter_name_fragment}"
        ),
        total_html=f"<strong>Total issues:</strong> {total_issues}",
        guidance_html="<p>Review all generated notes for accuracy before wider sharing.</p>",
    )
    info_section = _build_info_panel(panel_body)

    sections = []
    for (
        issue_key,
        summary,
        assignee_name,
        assignee_url,
        reporter_name,
        priority_name,
        labels,
        components,
        status,
        is_impediment,
        product,
        customer,
        llm_text,
        should_panel,
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
        labels_html = _format_labels(labels)
        components_html = (
            ", ".join(html.escape(component) for component in components)
            if components
            else "None"
        )
        issue_heading = f"<h3><a href=\"{html.escape(url)}\">{safe_key}</a>: {safe_summary}</h3>"
        flag_html = _impediment_badge() if is_impediment else ""
        assignee_line = (
            "<p>"
            f"{flag_html}"
            f"<strong>Status:</strong> {_format_status_value(status)} | "
            f"<strong>Assignee:</strong> {assignee_html} | "
            f"<strong>Priority:</strong> {priority_html} | "
            f"<strong>Labels:</strong> {labels_html} | "
            f"<strong>Components:</strong> {components_html} | "
            f"<strong>Reporter:</strong> {reporter_html}"
            "</p>"
        )
        product_html = html.escape(product or "Unknown")
        customer_html = html.escape(customer or "Unknown")
        product_customer_line = (
            "<p>"
            f"<strong>Product:</strong> {product_html} | "
            f"<strong>Customer:</strong> {customer_html}"
            "</p>"
        )
        safe_body = _render_markdown(llm_text)
        if should_panel:
            safe_body = _wrap_panel(safe_body)
        section = "".join([issue_heading, assignee_line, product_customer_line, safe_body])
        sections.append(section)

    return toc_macro + info_section + "".join(sections)


def _render_markdown(text: str) -> str:
    converted = markdown.markdown(
        text or "",
        extensions=["tables", "fenced_code"],
    )
    return converted


def _build_info_panel(text: str) -> str:
    if not text:
        return ""
    return (
        '<ac:structured-macro ac:name="info">'
        "<ac:parameter ac:name=\"icon\">information</ac:parameter>"
        "<ac:rich-text-body>"
        f"{text}"
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


def _format_labels(labels: Tuple[str, ...]) -> str:
    if not labels:
        return "None"
    formatted = []
    for label in labels:
        color = LABEL_STATUS_MAP.get(label)
        if color:
            formatted.append(
                '<ac:structured-macro ac:name="status">'
                f'<ac:parameter ac:name="colour">{html.escape(color)}</ac:parameter>'
                f'<ac:parameter ac:name="title">{html.escape(label)}</ac:parameter>'
                '<ac:parameter ac:name="subtle">false</ac:parameter>'
                "</ac:structured-macro>"
            )
        else:
            formatted.append(html.escape(label))
    return ", ".join(formatted)


def _format_status_value(status: str) -> str:
    if not status:
        return "Unknown"
    normalized = status.strip()
    if not normalized:
        return "Unknown"
    if normalized.lower() in _DONE_STATUS_NAMES:
        safe = html.escape(normalized)
        return (
            '<ac:structured-macro ac:name="status">'
            '<ac:parameter ac:name="colour">Green</ac:parameter>'
            f'<ac:parameter ac:name="title">{safe}</ac:parameter>'
            '<ac:parameter ac:name="subtle">false</ac:parameter>'
            "</ac:structured-macro>"
        )
    return html.escape(normalized)


def _assemble_info_panel_body(
    info_html: str,
    generated_html: str,
    filter_html: str,
    total_html: str,
    guidance_html: str,
) -> str:
    soup = BeautifulSoup("", "html.parser")
    container = soup.new_tag("div")

    if info_html:
        info_fragment = BeautifulSoup(info_html, "html.parser")
        for child in info_fragment.contents:
            container.append(child)

    row = soup.new_tag("p")
    row.append(BeautifulSoup(f"{generated_html} | ", "html.parser"))
    row.append(BeautifulSoup(f"{filter_html} | ", "html.parser"))
    row.append(BeautifulSoup(total_html, "html.parser"))
    container.append(row)

    if guidance_html:
        guidance_fragment = BeautifulSoup(guidance_html, "html.parser")
        for child in guidance_fragment.contents:
            container.append(child)

    return str(container)


def _wrap_panel(body_html: str) -> str:
    if not body_html:
        body_html = "<p></p>"
    return (
        '<ac:structured-macro ac:name="panel">'
        '<ac:parameter ac:name="borderColor">#0052CC</ac:parameter>'
        '<ac:parameter ac:name="borderStyle">solid</ac:parameter>'
        '<ac:parameter ac:name="bgColor">#E9F2FF</ac:parameter>'
        "<ac:rich-text-body>"
        f"{body_html}"
        "</ac:rich-text-body>"
        "</ac:structured-macro>"
    )
