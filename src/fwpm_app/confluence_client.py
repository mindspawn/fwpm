from __future__ import annotations

import logging
from typing import Dict, Optional

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
        existing = self._fetch_page(space_key, title, parent_page_id)
        if existing:
            return self._update_page(existing, parent_page_id, body_storage)
        return self._create_page(space_key, parent_page_id, title, body_storage)

    def _create_page(
        self, space_key: str, parent_page_id: int, title: str, body_storage: str
    ) -> Dict:
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
        return self._handle_response(response, url)

    def _update_page(self, page: Dict, parent_page_id: int, body_storage: str) -> Dict:
        page_id = page["id"]
        current_version = page.get("version", {}).get("number", 0)
        url = f"{self.base_url}/rest/api/content/{page_id}"
        payload = {
            "id": page_id,
            "type": "page",
            "title": page.get("title"),
            "space": {"key": page.get("space", {}).get("key")},
            "ancestors": [{"id": parent_page_id}],
            "version": {"number": current_version + 1},
            "body": {
                "storage": {
                    "value": body_storage,
                    "representation": "storage",
                }
            },
        }

        logger.info("Confluence PUT %s", url)
        response = self.session.put(url, json=payload, timeout=self.timeout)
        return self._handle_response(response, url)

    def _fetch_page(
        self, space_key: str, title: str, parent_page_id: int
    ) -> Optional[Dict]:
        url = f"{self.base_url}/rest/api/content"
        params = {
            "spaceKey": space_key,
            "title": title,
            "expand": "version,ancestors,space",
            "limit": 10,
        }

        logger.info("Confluence GET %s params=%s", url, params)
        response = self.session.get(url, params=params, timeout=self.timeout)
        data = self._handle_response(response, url)
        results = data.get("results", []) if isinstance(data, dict) else []

        for page in results:
            ancestors = page.get("ancestors", []) or []
            if any(str(ancestor.get("id")) == str(parent_page_id) for ancestor in ancestors):
                return page
        return results[0] if results else None

    def _handle_response(self, response: requests.Response, url: str) -> Dict:
        logger.info("Confluence response %s %s", response.status_code, url)
        if not response.ok:
            try:
                logger.error("Confluence error payload: %s", response.json())
            except ValueError:
                logger.error("Confluence error payload (raw): %s", response.text)
            response.raise_for_status()
        return response.json()

    def get_page_view_html(self, page_id: str) -> Dict:
        url = f"{self.base_url}/rest/api/content/{page_id}"
        params = {"expand": "body.view"}
        logger.info("Confluence GET %s params=%s", url, params)
        response = self.session.get(url, params=params, timeout=self.timeout)
        return self._handle_response(response, url)
