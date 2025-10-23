"""
Default configuration values for the fwpm application.

Update the placeholders below with your organization's credentials and endpoints
to avoid passing environment variables on each run.
"""

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
    "HTTP_VERIFY_SSL": "true",
    "HTTP_REQUEST_TIMEOUT": "30",
}

# Jira account identifiers whose comments should be ignored when preparing issue text.
# Populate with values such as account IDs, usernames, or emails that appear in comment author details.
IGNORE_COMMENTS_FROM = {
    # "5d1234567890abcdef123456",
    # "automation-bot",
}

# Directory used to store generated Jira text snapshots (`<ISSUE-KEY>.txt`).
# Customize this if you prefer a different location.
ISSUE_TEXT_OUTPUT_DIR = "issue_text_debug"

# Optional delay (seconds) between consecutive LLM prompts.
# Helpful for rate-limited local models; set to 0 to disable throttling.
LLM_REQUEST_DELAY_SECONDS = 0
