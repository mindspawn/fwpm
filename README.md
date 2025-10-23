# Repository Guidelines

This repository contains a containerised Python application that fetches Jira issues, generates LLM summaries, and publishes results to Confluence.

## Prerequisites

- Docker 20.10+
- Access to Jira Data Center v9 and Confluence with REST APIs enabled
- OpenAI-compatible LLM endpoint reachable from the container host

## Configuration

1. Edit `src/fwpm_app/defaults.py` to include your Jira, Confluence, and LLM credentials/endpoints.
   - Any environment variable set at runtime overrides the value from `defaults.py`.
2. Optional overrides can be provided with `docker run -e VAR=value`.

Required settings:
- `JIRA_BASE_URL`, `JIRA_USERNAME`, `JIRA_API_TOKEN`
- `CONFLUENCE_BASE_URL`, `CONFLUENCE_USERNAME`, `CONFLUENCE_API_TOKEN`
- `LLM_BASE_URL`, `LLM_API_KEY`, optionally `LLM_MODEL`
- Networking defaults: `HTTP_VERIFY_SSL`, `HTTP_REQUEST_TIMEOUT`

Ensure each Jira filter you target has YAML configuration in the filter description:

```yaml
confluence:
  space_key: ENG
  parent_page_id: 123456
  page_name: Automated LLM Summary
llm:
  prompt: |
    Read the provided context and generate a structured summary.
    Focus on key decisions, blockers, and next steps.
```

## Build

```bash
docker build -t fwpm-app .
```

## Run Modes

### Full Workflow

```bash
docker run --rm fwpm-app <filter_id>
```

The application will:
1. Fetch the filter details and issues from Jira.
2. Parse the YAML configuration.
3. Generate LLM summaries.
4. Publish a Confluence page using the YAML-specified location/title.

### List-Only (Dry Run)

```bash
docker run --rm fwpm-app <filter_id> --list-only
```

Outputs each Jira key and summary without invoking the LLM or Confluence.

### Confluence Placeholder

```bash
docker run --rm fwpm-app <filter_id> --confluence-placeholder
```

Creates the Confluence page but substitutes a placeholder string for the LLM response.

## Logging & Metrics

- Requests to Jira, the LLM endpoint, and Confluence are logged with status codes.
- The total count of Jira issues and total time spent on LLM calls are recorded.
- Use `--log-level DEBUG` for verbose logs.

## Extensibility

Key modules under `src/fwpm_app` are designed for replacement or extension:

- `issue_content.py` – customize the text representation of Jira issues.
- `workflow.py` – orchestrates fetching, LLM processing, and publishing; exposes helper methods for test modes.
- `defaults.py` – central place for default configuration.

To run against a different LLM or change Confluence formatting, modify the corresponding client or renderer modules.
