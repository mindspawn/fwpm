from __future__ import annotations

import dataclasses
import os
import logging
from functools import lru_cache
from typing import Any, Dict, Optional

import yaml

from .defaults import DEFAULT_SETTINGS, SYSTEM_PROMPT_FILE, USE_SYSTEM_PROMPT_FILE


@dataclasses.dataclass
class ConfluenceConfig:
    space_key: str
    parent_page_id: int
    page_name: str


@dataclasses.dataclass
class LLMConfig:
    model: str
    system_prompt: str
    temperature: float
    top_p: float
    frequency_penalty: float
    presence_penalty: float


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
    llm_temperature: float
    llm_top_p: float
    llm_frequency_penalty: float
    llm_presence_penalty: float
    llm_system_prompt: str
    llm_allow_prompt_override: bool
    llm_use_system_prompt_file: bool
    llm_user_prompt: str
    comment_lookback_hours: int
    include_description_background: bool
    include_older_comments_background: bool
    confluence_validate_html: bool
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

        use_prompt_file = _as_bool(optional("LLM_USE_SYSTEM_PROMPT_FILE", "false"))
        system_prompt_env = optional("LLM_SYSTEM_PROMPT")
        if system_prompt_env is not None:
            system_prompt = system_prompt_env
        elif use_prompt_file:
            system_prompt = _load_default_system_prompt()
        else:
            system_prompt = ""

        try:
            lookback_hours = int(optional("COMMENT_LOOKBACK_HOURS", "24"))
        except ValueError as exc:
            raise RuntimeError("COMMENT_LOOKBACK_HOURS must be an integer") from exc

        include_description_bg = _as_bool(optional("INCLUDE_DESCRIPTION_IN_BACKGROUND", "true"))
        include_older_comments_bg = _as_bool(optional("INCLUDE_OLDER_COMMENTS_IN_BACKGROUND", "false"))

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
            llm_temperature=float(optional("LLM_TEMPERATURE", "0.2")),
            llm_top_p=float(optional("LLM_TOP_P", "0.9")),
            llm_frequency_penalty=float(optional("LLM_FREQUENCY_PENALTY", "0")),
            llm_presence_penalty=float(optional("LLM_PRESENCE_PENALTY", "0.1")),
            llm_system_prompt=system_prompt,
            llm_allow_prompt_override=_as_bool(optional("LLM_ALLOW_PROMPT_OVERRIDE", "false")),
            llm_use_system_prompt_file=use_prompt_file,
            llm_user_prompt=optional("LLM_USER_PROMPT", DEFAULT_SETTINGS["LLM_USER_PROMPT"]),
            comment_lookback_hours=lookback_hours,
            include_description_background=include_description_bg,
            include_older_comments_background=include_older_comments_bg,
            confluence_validate_html=_as_bool(optional("CONFLUENCE_VALIDATE_HTML", "true")),
            verify_ssl=verify_ssl,
            request_timeout=timeout,
        )


def parse_filter_description(description: Optional[str], defaults: AppConfig) -> FilterConfig:
    if not description:
        raise RuntimeError("Filter description is empty; expected YAML configuration.")

    try:
        data = yaml.safe_load(description)
    except yaml.YAMLError as exc:
        raise RuntimeError("Failed to parse filter description as YAML.") from exc

    if not isinstance(data, dict):
        raise RuntimeError("Filter description YAML must be a mapping.")

    confluence_section = _ensure_section(data, "confluence")
    llm_section_raw = data.get("llm", {})
    if llm_section_raw is None:
        llm_section_raw = {}
    if not isinstance(llm_section_raw, dict):
        raise RuntimeError("Filter description 'llm' section must be a mapping if provided.")
    llm_section = llm_section_raw

    confluence = ConfluenceConfig(
        space_key=_require_str(confluence_section, "space_key"),
        parent_page_id=_require_int(confluence_section, "parent_page_id"),
        page_name=_require_str(confluence_section, "page_name"),
    )

    requested_system_prompt = llm_section.get("system_prompt")
    if requested_system_prompt and not defaults.llm_allow_prompt_override:
        logger.debug(
            "System prompt override specified in filter but disabled by configuration; using default."
        )
    if defaults.llm_allow_prompt_override and requested_system_prompt is not None:
        system_prompt = requested_system_prompt
    else:
        system_prompt = defaults.llm_system_prompt

    llm = LLMConfig(
        model=llm_section.get("model", defaults.llm_model),
        system_prompt=system_prompt,
        temperature=_require_float(llm_section, "temperature", defaults.llm_temperature),
        top_p=_require_float(llm_section, "top_p", defaults.llm_top_p),
        frequency_penalty=_require_float(
            llm_section, "frequency_penalty", defaults.llm_frequency_penalty
        ),
        presence_penalty=_require_float(
            llm_section, "presence_penalty", defaults.llm_presence_penalty
        ),
    )

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


def _require_float(data: Dict[str, Any], key: str, default: float) -> float:
    value = data.get(key)
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        raise RuntimeError(f"Expected '{key}' to be a float in filter YAML.")


@lru_cache(maxsize=1)
def _load_default_system_prompt() -> str:
    try:
        content = SYSTEM_PROMPT_FILE.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"System prompt file not found at {SYSTEM_PROMPT_FILE}."
        ) from exc
    content = content.strip()
    if not content:
        raise RuntimeError(
            f"System prompt file {SYSTEM_PROMPT_FILE} is empty."
        )
    return content


def _as_bool(value: Optional[str]) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


logger = logging.getLogger(__name__)
