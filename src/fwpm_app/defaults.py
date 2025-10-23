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
