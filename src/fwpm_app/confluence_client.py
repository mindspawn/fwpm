from __future__ import annotations

import logging
from typing import Dict

import requests

logger = logging.getLogger(__name__)


class ConfluenceClient:
    def __init__(
        self,
        base_url: str,
        username: str,
        api_token: str,
        timeout: int = 30,
        verify_ssl: bool = True,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.auth = (username, api_token)
        self.session.verify = verify_ssl

    def create_page(self, space_key: str, parent_page_id: int, title: str, body_storage: str) -> Dict:
        url = f"{self.base_url}/rest/api/content"
        payload = {
            "type": "page",
            "title": title,
            "ancestors": [{"id": parent_page_id}],
            "space": {"key": space_key},
            "body": {
                "storage": {
                    "value": body_storage,
                    "representation": "storage",
                }
            },
        }

        logger.info("Confluence POST %s", url)
        response = self.session.post(url, json=payload, timeout=self.timeout)
        logger.info("Confluence response %s %s", response.status_code, url)
        if not response.ok:
            try:
                logger.error(
                    "Confluence error payload: %s",
                    response.json(),
                )
            except ValueError:
                logger.error("Confluence error payload (raw): %s", response.text)
            response.raise_for_status()
        return response.json()
