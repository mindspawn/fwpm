from __future__ import annotations

import argparse
import logging
import sys

from .config import AppConfig
from .confluence_client import ConfluenceClient
from .jira_client import JiraClient
from .llm_client import LLMClient
from .workflow import Workflow


def configure_logging(level_name: str) -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate LLM summaries for a JIRA filter.")
    parser.add_argument("filter_id", help="Numeric or string identifier of the JIRA filter.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO",
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="Test mode: only fetch and print issues returned by the filter.",
    )
    parser.add_argument(
        "--confluence-placeholder",
        action="store_true",
        help="Test mode: publish to Confluence with placeholder LLM text (no LLM calls).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of issues to process from the filter (applies to all modes).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    configure_logging(args.log_level)

    try:
        app_config = AppConfig.from_env()
    except RuntimeError as exc:
        logging.getLogger(__name__).error(str(exc))
        return 1

    jira_client = JiraClient(
        base_url=app_config.jira_base_url,
        username=app_config.jira_username,
        api_token=app_config.jira_api_token,
        timeout=app_config.request_timeout,
        verify_ssl=app_config.verify_ssl,
    )

    llm_client = LLMClient(
        base_url=app_config.llm_base_url,
        api_key=app_config.llm_api_key,
        model=app_config.llm_model,
        timeout=app_config.request_timeout,
        verify_ssl=app_config.verify_ssl,
    )

    confluence_client = ConfluenceClient(
        base_url=app_config.confluence_base_url,
        username=app_config.confluence_username,
        api_token=app_config.confluence_api_token,
        timeout=app_config.request_timeout,
        verify_ssl=app_config.verify_ssl,
    )

    workflow = Workflow(
        app_config=app_config,
        jira_client=jira_client,
        llm_client=llm_client,
        confluence_client=confluence_client,
    )

    try:
        if args.list_only and args.confluence_placeholder:
            logging.getLogger(__name__).error(
                "Choose at most one of --list-only or --confluence-placeholder."
            )
            return 1
        if args.limit is not None and args.limit <= 0:
            logging.getLogger(__name__).error("--limit must be a positive integer.")
            return 1

        if args.list_only:
            filter_details, issues = workflow.collect_issues(args.filter_id)
            filter_name = filter_details.get("name", "")
            total = len(issues)
            if args.limit is not None:
                issues = issues[: args.limit]
            processed = len(issues)
            summary = (
                f"Filter {args.filter_id} ({filter_name}) returned {total} issues"
                if args.limit is None
                else f"Filter {args.filter_id} ({filter_name}) returned {total} issues; "
                f"showing first {processed}"
            )
            print(summary + ":")
            for issue in issues:
                summary = issue.get("fields", {}).get("summary", "") or "<no summary>"
                print(f"- {issue.get('key')}: {summary}")
        elif args.confluence_placeholder:
            workflow.run_with_placeholder(args.filter_id, limit=args.limit)
        else:
            workflow.run(args.filter_id, limit=args.limit)
    except Exception as exc:  # pragma: no cover - top-level guard
        logging.getLogger(__name__).exception("Workflow failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
