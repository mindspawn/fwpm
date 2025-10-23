from __future__ import annotations

import logging
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


class JiraClient:
    def __init__(
        self,
        base_url: str,
        username: str,
        api_token: str,
        timeout: int = 30,
        verify_ssl: bool = True,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.auth = (username, api_token)
        self.session.verify = verify_ssl
        self.timeout = timeout

    def get_filter(self, filter_id: str) -> Dict:
        path = f"/rest/api/2/filter/{filter_id}"
        return self._request("GET", path)

    def search_issues(self, jql: str, fields: Optional[List[str]] = None) -> List[Dict]:
        issues: List[Dict] = []
        start_at = 0
        max_results = 100

        params = {"jql": jql, "startAt": start_at, "maxResults": max_results}
        if fields:
            params["fields"] = ",".join(fields)

        while True:
            params["startAt"] = start_at
            data = self._request("GET", "/rest/api/2/search", params=params)

            batch = data.get("issues", [])
            issues.extend(batch)

            total = data.get("total", len(issues))
            logger.info(
                "JIRA search page start=%s retrieved %s issues (total=%s)",
                start_at,
                len(batch),
                total,
            )

            start_at += len(batch)
            if start_at >= total or not batch:
                break

        logger.info("JIRA search completed: %s issues returned for JQL '%s'", len(issues), jql)
        return issues

    def _request(self, method: str, path: str, **kwargs) -> Dict:
        url = f"{self.base_url}{path}"
        logger.info("JIRA %s %s", method.upper(), url)
        response = self.session.request(method, url, timeout=self.timeout, **kwargs)
        logger.info("JIRA response %s %s", response.status_code, url)
        response.raise_for_status()
        return response.json()
