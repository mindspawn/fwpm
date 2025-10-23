from __future__ import annotations

import dataclasses
import os
from typing import Any, Dict, Optional

import yaml

from .defaults import DEFAULT_SETTINGS


@dataclasses.dataclass
class ConfluenceConfig:
    space_key: str
    parent_page_id: int
    page_name: str


@dataclasses.dataclass
class LLMConfig:
    prompt: str
    model: str


@dataclasses.dataclass
class FilterConfig:
    confluence: ConfluenceConfig
    llm: LLMConfig


@dataclasses.dataclass
class AppConfig:
    jira_base_url: str
    jira_username: str
    jira_api_token: str
    confluence_base_url: str
    confluence_username: str
    confluence_api_token: str
    llm_base_url: str
    llm_api_key: str
    llm_model: str
    verify_ssl: bool = True
    request_timeout: int = 30

    @classmethod
    def from_env(cls) -> "AppConfig":
        def require(name: str) -> str:
            value = os.getenv(name)
            if value:
                return value
            default = DEFAULT_SETTINGS.get(name)
            if default:
                return default
            raise RuntimeError(f"Configuration value for {name} is required")

        def optional(name: str, fallback: Optional[str] = None) -> Optional[str]:
            value = os.getenv(name)
            if value:
                return value
            default = DEFAULT_SETTINGS.get(name)
            if default:
                return default
            return fallback

        verify_ssl_raw = optional("HTTP_VERIFY_SSL", "true")
        verify_ssl = str(verify_ssl_raw).lower() in {"1", "true", "yes"}

        timeout_raw = optional("HTTP_REQUEST_TIMEOUT", "30")
        try:
            timeout = int(timeout_raw)
        except ValueError as exc:  # pragma: no cover - defensive
            raise RuntimeError("HTTP_REQUEST_TIMEOUT must be an integer") from exc

        return cls(
            jira_base_url=require("JIRA_BASE_URL"),
            jira_username=require("JIRA_USERNAME"),
            jira_api_token=require("JIRA_API_TOKEN"),
            confluence_base_url=optional("CONFLUENCE_BASE_URL", require("JIRA_BASE_URL")),
            confluence_username=optional("CONFLUENCE_USERNAME", require("JIRA_USERNAME")),
            confluence_api_token=optional("CONFLUENCE_API_TOKEN", require("JIRA_API_TOKEN")),
            llm_base_url=require("LLM_BASE_URL"),
            llm_api_key=require("LLM_API_KEY"),
            llm_model=optional("LLM_MODEL", "gpt-3.5-turbo"),
            verify_ssl=verify_ssl,
            request_timeout=timeout,
        )


def parse_filter_description(description: Optional[str], default_model: str) -> FilterConfig:
    if not description:
        raise RuntimeError("Filter description is empty; expected YAML configuration.")

    try:
        data = yaml.safe_load(description)
    except yaml.YAMLError as exc:
        raise RuntimeError("Failed to parse filter description as YAML.") from exc

    if not isinstance(data, dict):
        raise RuntimeError("Filter description YAML must be a mapping.")

    confluence_section = _ensure_section(data, "confluence")
    llm_section = _ensure_section(data, "llm")

    confluence = ConfluenceConfig(
        space_key=_require_str(confluence_section, "space_key"),
        parent_page_id=_require_int(confluence_section, "parent_page_id"),
        page_name=_require_str(confluence_section, "page_name"),
    )

    prompt = _require_str(llm_section, "prompt")

    llm = LLMConfig(prompt=prompt, model=llm_section.get("model", default_model))

    return FilterConfig(confluence=confluence, llm=llm)


def _ensure_section(data: Dict[str, Any], key: str) -> Dict[str, Any]:
    value = data.get(key)
    if not isinstance(value, dict):
        raise RuntimeError(f"Filter YAML must include a '{key}' mapping.")
    return value


def _require_str(data: Dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not value or not isinstance(value, str):
        raise RuntimeError(f"Expected '{key}' to be a non-empty string in filter YAML.")
    return value.strip()


def _require_int(data: Dict[str, Any], key: str) -> int:
    value = data.get(key)
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    raise RuntimeError(f"Expected '{key}' to be an integer in filter YAML.")
