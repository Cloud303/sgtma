from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Type
import os
import time
import requests

from pydantic import BaseModel, Field, SecretStr, HttpUrl
from demo_flags import DEMO_MODE


# ==============================================================================
# Error hierarchy
# ==============================================================================

class SocialMediaError(Exception):
    """Base exception for social media integration errors."""


class AuthenticationError(SocialMediaError):
    """Raised when authentication or token refresh fails."""


class AuthorizationError(SocialMediaError):
    """Raised when the API rejects the request due to insufficient permissions."""


class RateLimitError(SocialMediaError):
    """Raised when the API rate limit is reached."""

    def __init__(self, message: str, retry_after_seconds: Optional[int] = None) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class APIRequestError(SocialMediaError):
    """Raised for non-recoverable API request errors."""


class ValidationError(SocialMediaError):
    """Raised when input payloads are invalid for a given platform."""


# ==============================================================================
# Data models
# ==============================================================================

class PlatformConfig(BaseModel):
    """Configuration required to operate a platform integration."""

    platform_name: str = Field(..., description="Canonical platform key, e.g., 'twitter', 'linkedin'.")
    api_base_url: Optional[str] = Field(default=None, description="Base URL for the platform API.")

    # OAuth / App credentials (optional depending on platform)
    client_id: Optional[str] = None
    client_secret: Optional[SecretStr] = None

    # User/Account tokens
    access_token: Optional[SecretStr] = None
    refresh_token: Optional[SecretStr] = None
    token_expires_at: Optional[datetime] = None

    # Operational settings
    rate_limit_per_minute: int = Field(default=60, ge=1)
    default_timeout_seconds: int = Field(default=15, ge=1)
    webhook_url: Optional[HttpUrl] = None

    # Free-form extras per platform
    extra: Dict[str, Any] = Field(default_factory=dict)


class PostResult(BaseModel):
    platform: str
    post_id: str
    url: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    raw_response: Optional[Dict[str, Any]] = None


class MetricsResult(BaseModel):
    platform: str
    post_id: Optional[str] = None
    metrics: Dict[str, Any]
    fetched_at: datetime = Field(default_factory=datetime.utcnow)
    raw_response: Optional[Dict[str, Any]] = None


# ==============================================================================
# Abstract base class for platforms
# ==============================================================================

class SocialMediaPlatform(ABC):
    """Abstract base for all social media platform implementations."""

    def __init__(self, config: PlatformConfig) -> None:
        self.config = config
        self._session = requests.Session()

    # ---------- Configuration ----------
    def validate_config(self) -> None:
        """Validate configuration is sufficient for this platform."""
        if not self.config.platform_name:
            raise ValidationError("platform_name is required")

    # ---------- Authentication ----------
    @abstractmethod
    def authenticate(self) -> None:
        """Acquire or refresh access tokens as required by the platform."""
        raise NotImplementedError

    def is_token_expired(self) -> bool:
        expires = self.config.token_expires_at
        if not expires:
            return False
        # Consider tokens expiring within 60s as expired to avoid race conditions
        return datetime.utcnow() >= (expires - timedelta(seconds=60))

    # ---------- Core operations ----------
    @abstractmethod
    def post_content(
        self,
        content_text: str,
        media_urls: Optional[List[str]] = None,
        scheduled_for: Optional[datetime] = None,
    ) -> PostResult:
        """Publish or schedule content on the platform."""
        raise NotImplementedError

    @abstractmethod
    def fetch_metrics(
        self,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        post_id: Optional[str] = None,
    ) -> List[MetricsResult]:
        """Retrieve analytics metrics for posts or account over a window."""
        raise NotImplementedError

    # ---------- Connectivity ----------
    @abstractmethod
    def test_connection(self) -> bool:
        """Quick health check against the platform API."""
        raise NotImplementedError

    def disconnect(self) -> None:
        """Cleanup any resources; default is to close the HTTP session."""
        self._session.close()

    # ---------- Utilities ----------
    def request_with_retry(
        self,
        method: str,
        url: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        files: Optional[Dict[str, Any]] = None,
        expected_statuses: Optional[List[int]] = None,
        max_retries: int = 3,
        backoff_seconds: float = 1.0,
        timeout_seconds: Optional[int] = None,
    ) -> requests.Response:
        """HTTP request with basic retry/backoff and rate-limit handling."""
        
        # Disable actual network requests in demo mode
        if DEMO_MODE:
            raise RuntimeError("Network requests disabled in demo mode.")
        
        if expected_statuses is None:
            expected_statuses = [200, 201, 202]

        timeout = timeout_seconds or self.config.default_timeout_seconds

        last_exc: Optional[Exception] = None
        for attempt in range(1, max_retries + 1):
            try:
                response = self._session.request(
                    method=method.upper(),
                    url=url,
                    headers=headers,
                    params=params,
                    json=json,
                    data=data,
                    files=files,
                    timeout=timeout,
                )

                # Handle rate limit
                if response.status_code == 429:
                    retry_after = self._parse_retry_after_seconds(response)
                    if attempt == max_retries:
                        raise RateLimitError(
                            f"Rate limited after {attempt} attempts", retry_after_seconds=retry_after
                        )
                    time.sleep(retry_after or backoff_seconds * attempt)
                    continue

                # Auth issues
                if response.status_code in (401, 403):
                    if attempt == 1:  # try refresh once before failing hard
                        try:
                            self.authenticate()
                        except Exception as auth_exc:  # noqa: BLE001
                            raise AuthenticationError(str(auth_exc)) from auth_exc
                        # retry immediately after refresh
                        continue
                    else:
                        raise AuthorizationError(
                            f"Authorization failed with status {response.status_code}: {response.text}"
                        )

                # Success statuses
                if response.status_code in expected_statuses:
                    return response

                # Other errors: retry on 5xx
                if 500 <= response.status_code < 600 and attempt < max_retries:
                    time.sleep(backoff_seconds * attempt)
                    continue

                # Non-retryable
                raise APIRequestError(
                    f"Unexpected status {response.status_code}: {response.text[:300]}"
                )

            except (requests.Timeout, requests.ConnectionError) as exc:  # network issues
                last_exc = exc
                if attempt == max_retries:
                    raise APIRequestError(f"Network error after {attempt} attempts: {exc}") from exc
                time.sleep(backoff_seconds * attempt)
                continue

        # Should not reach here
        if last_exc:
            raise APIRequestError(f"Request failed: {last_exc}")
        raise APIRequestError("Request failed for unknown reasons")

    def _parse_retry_after_seconds(self, response: requests.Response) -> Optional[int]:
        header = response.headers.get("Retry-After")
        if not header:
            return None
        try:
            return int(header)
        except ValueError:
            return None

    def map_platform_metrics(self, raw_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Override to normalize raw metrics into a common schema."""
        return raw_metrics


# ==============================================================================
# Service manager and registry
# ==============================================================================

class SocialMediaService:
    """Coordinator for social platform integrations."""

    def __init__(self) -> None:
        self._registry: Dict[str, Type[SocialMediaPlatform]] = {}
        self._instances: Dict[str, SocialMediaPlatform] = {}

    # ---------- Registration ----------
    def register_platform(self, platform_name: str, platform_cls: Type[SocialMediaPlatform]) -> None:
        key = platform_name.lower()
        self._registry[key] = platform_cls

    def is_registered(self, platform_name: str) -> bool:
        return platform_name.lower() in self._registry

    # ---------- Instance management ----------
    def add_account(self, config: PlatformConfig) -> None:
        key = config.platform_name.lower()
        if key not in self._registry:
            raise ValidationError(f"Platform not registered: {config.platform_name}")
        platform_cls = self._registry[key]
        instance = platform_cls(config)
        instance.validate_config()
        self._instances[key] = instance

    def get_platform(self, platform_name: str) -> SocialMediaPlatform:
        key = platform_name.lower()
        if key in self._instances:
            return self._instances[key]
        raise ValidationError(f"No configured account for platform: {platform_name}")

    # ---------- Delegated operations ----------
    def post(
        self,
        platform_name: str,
        *,
        content_text: str,
        media_urls: Optional[List[str]] = None,
        scheduled_for: Optional[datetime] = None,
    ) -> PostResult:
        if DEMO_MODE:
            # Return placeholder result in demo mode
            return PostResult(
                platform=platform_name,
                post_id=f"demo_post_{platform_name}_123",
                url=f"https://redacted.example.com/{platform_name}/demo_post_123",
                raw_response={"demo": True}
            )
        
        platform = self.get_platform(platform_name)
        if platform.is_token_expired():
            platform.authenticate()
        return platform.post_content(
            content_text=content_text,
            media_urls=media_urls,
            scheduled_for=scheduled_for,
        )

    def fetch_metrics(
        self,
        platform_name: str,
        *,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        post_id: Optional[str] = None,
    ) -> List[MetricsResult]:
        if DEMO_MODE:
            # Return empty metrics in demo mode
            return []
        
        platform = self.get_platform(platform_name)
        if platform.is_token_expired():
            platform.authenticate()
        return platform.fetch_metrics(since=since, until=until, post_id=post_id)


# ==============================================================================
# Configuration helpers
# ==============================================================================

ENV_MAP = {
    "client_id": "CLIENT_ID",
    "client_secret": "CLIENT_SECRET",
    "access_token": "ACCESS_TOKEN",
    "refresh_token": "REFRESH_TOKEN",
    "api_base_url": "API_BASE_URL",
    "webhook_url": "WEBHOOK_URL",
    "rate_limit_per_minute": "RATE_LIMIT_PER_MINUTE",
    "default_timeout_seconds": "DEFAULT_TIMEOUT_SECONDS",
}


def load_platform_config_from_env(platform_name: str, *, prefix: Optional[str] = None) -> PlatformConfig:
    """Load a platform configuration from environment variables.

    Variables are resolved using either the provided prefix (e.g., "TWITTER") or a derived
    prefix from the platform name (e.g., platform_name="twitter" -> prefix "TWITTER").

    Recognized variables include CLIENT_ID, CLIENT_SECRET, ACCESS_TOKEN, REFRESH_TOKEN, API_BASE_URL,
    WEBHOOK_URL, RATE_LIMIT_PER_MINUTE, DEFAULT_TIMEOUT_SECONDS.
    """
    key_prefix = (prefix or platform_name).upper()

    def env(name: str) -> Optional[str]:
        return os.getenv(f"{key_prefix}_{name}")

    extra: Dict[str, Any] = {}

    client_secret = env(ENV_MAP["client_secret"]) or None
    access_token = env(ENV_MAP["access_token"]) or None
    refresh_token = env(ENV_MAP["refresh_token"]) or None

    rate_limit_str = env(ENV_MAP["rate_limit_per_minute"]) or "60"
    timeout_str = env(ENV_MAP["default_timeout_seconds"]) or "15"

    return PlatformConfig(
        platform_name=platform_name,
        api_base_url=env(ENV_MAP["api_base_url"]) or None,
        client_id=env(ENV_MAP["client_id"]) or None,
        client_secret=SecretStr(client_secret) if client_secret else None,
        access_token=SecretStr(access_token) if access_token else None,
        refresh_token=SecretStr(refresh_token) if refresh_token else None,
        rate_limit_per_minute=int(rate_limit_str),
        default_timeout_seconds=int(timeout_str),
        webhook_url=env(ENV_MAP["webhook_url"]) or None,
        extra=extra,
    )


# Singleton-style service export for convenience in the rest of the app
social_media_service = SocialMediaService() 