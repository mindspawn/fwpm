"""
Default configuration values for the fwpm application.

Update the placeholders below with your organization's credentials and endpoints
 to avoid passing environment variables on each run.
"""

from pathlib import Path

DEFAULT_SETTINGS = {
    "JIRA_BASE_URL": "",
    "JIRA_USERNAME": "",
    "JIRA_API_TOKEN": "",
    "CONFLUENCE_BASE_URL": "",
    "CONFLUENCE_USERNAME": "",
    "CONFLUENCE_API_TOKEN": "",
    "LLM_BASE_URL": "",
    "LLM_API_KEY": "",
    "LLM_MODEL": "gpt-3.5-turbo",
    "LLM_TEMPERATURE": "0.2",
    "LLM_TOP_P": "0.9",
    "LLM_FREQUENCY_PENALTY": "0",
    "LLM_PRESENCE_PENALTY": "0.1",
    "LLM_USER_PROMPT": (
        "Summarize the issue focusing on key decisions, blockers, owners, "
        "risks, and next steps. Highlight any impediments requiring leadership "
        "attention."
    ),
    "COMMENT_LOOKBACK_HOURS": "24",
    "INCLUDE_DESCRIPTION_IN_BACKGROUND": "false",
    "INCLUDE_OLDER_COMMENTS_IN_BACKGROUND": "true",
    "LLM_ALLOW_PROMPT_OVERRIDE": "false",
    "HTTP_VERIFY_SSL": "true",
    "HTTP_REQUEST_TIMEOUT": "30",
    "CONFLUENCE_VALIDATE_HTML": "true",
}

# Jira account identifiers whose comments should be ignored when preparing issue text.
# Populate with values such as account IDs, usernames, or emails that appear in comment author details.
IGNORE_COMMENTS_FROM = {
    # "5d1234567890abcdef123456",
    # "automation-bot",
}

# Directory used to store generated Jira text snapshots (`<ISSUE-KEY>.txt`).
# Customize this if you prefer a different location.
OUTPUT_BASE_DIR = Path("output")
ISSUE_TEXT_OUTPUT_DIR = OUTPUT_BASE_DIR / "prompts"
LLM_RESPONSE_OUTPUT_DIR = OUTPUT_BASE_DIR / "responses"
CONFLUENCE_OUTPUT_FILE = OUTPUT_BASE_DIR / "confluence" / "page.html"

# Optional delay (seconds) between consecutive LLM prompts.
# Helpful for rate-limited local models; set to 0 to disable throttling.
LLM_REQUEST_DELAY_SECONDS = 0

# Optional informational banner injected at the top of the generated Confluence page.
# Use standard HTML/markdown here; the renderer will wrap it in a Confluence info macro.
INFO_HEADER = (
    "Verify these summaries before sharing outside the team."
    " Update the filter YAML prompt if focus areas change."
)


SYSTEM_PROMPT_FILE = Path("prompts/system_prompt.txt")
TRACE_NAME = "myapp"

# Whether to load the repository system prompt file by default.
USE_SYSTEM_PROMPT_FILE = False

# Mapping of label names to Confluence status macro colours.
# Example: {"Blocked": "Red", "On Track": "Green"}
LABEL_STATUS_MAP = {}
