from __future__ import annotations

import logging
from typing import Dict, Optional

import requests

logger = logging.getLogger(__name__)


class LLMClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        model: str,
        timeout: int = 30,
        completions_path: str = "/v1/chat/completions",
        verify_ssl: bool = True,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.timeout = timeout
        self.completions_path = completions_path
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        self.session.verify = verify_ssl

    def generate_completion(
        self,
        system_prompt: str,
        issue_text: str,
        *,
        temperature: float,
        top_p: float,
        frequency_penalty: float,
        presence_penalty: float,
    ) -> str:
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": issue_text},
            ],
            "temperature": temperature,
            "top_p": top_p,
            "frequency_penalty": frequency_penalty,
            "presence_penalty": presence_penalty,
        }

        url = f"{self.base_url}{self.completions_path}"
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}

        logger.info("LLM POST %s", url)
        response = self.session.post(url, json=payload, headers=headers, timeout=self.timeout)
        logger.info("LLM response %s %s", response.status_code, url)
        response.raise_for_status()

        data = response.json()
        content = _extract_content(data)
        if content is None:
            raise RuntimeError("LLM response did not include any content.")
        return content.strip()


def _extract_content(data: Dict) -> Optional[str]:
    choices = data.get("choices")
    if not choices:
        return None
    message = choices[0].get("message")
    if not isinstance(message, dict):
        return None
    return message.get("content")
