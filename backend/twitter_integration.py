from __future__ import annotations

import base64
import hashlib
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from oauthlib.oauth2 import WebApplicationClient
from pydantic import BaseModel, Field, SecretStr
import urllib.parse
from demo_flags import DEMO_MODE

from social_media_service import (
    SocialMediaPlatform,
    PlatformConfig,
    PostResult,
    MetricsResult,
    ValidationError,
    AuthenticationError,
    APIRequestError,
)


DEFAULT_API_BASE = "https://redacted.example.com"
AUTH_BASE = "https://redacted.example.com"
AUTHORIZATION_URL = f"{AUTH_BASE}/i/oauth2/authorize"
TOKEN_URL = "https://redacted.example.com"
REVOKE_URL = "https://redacted.example.com"


class TwitterAuthState(BaseModel):
    code_verifier: str
    code_challenge: str
    state: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


def generate_pkce_pair() -> Tuple[str, str]:
    """Generate PKCE verifier and challenge according to RFC 7636."""
    # Generate a high-entropy cryptographic random string
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode('utf-8').rstrip('=')
    # S256 challenge
    challenge = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode('utf-8')).digest()
    ).decode('utf-8').rstrip('=')
    return verifier, challenge


def format_rfc3339(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class TwitterPlatform(SocialMediaPlatform):
    """Twitter/X integration using OAuth 2.0 and v2 endpoints with OAuthLib."""

    def __init__(self, config: PlatformConfig) -> None:
        if not config.api_base_url:
            config.api_base_url = DEFAULT_API_BASE
        super().__init__(config)
        # Initialize OAuth2 client
        self.oauth_client = WebApplicationClient(config.client_id)

    # ---------- Authentication ----------
    def authenticate(self) -> None:
        if DEMO_MODE:
            raise RuntimeError("External auth disabled in demo.")
        # In standalone mode, we only support refresh if tokens are present.
        if self.config.refresh_token and self.is_token_expired():
            self.refresh_access_token()
        elif not self.config.access_token:
            raise AuthenticationError(
                "No access token present. Use authorization URL and token exchange to obtain one."
            )

    def build_authorization_url(
        self,
        *,
        scopes: Optional[List[str]] = None,
        redirect_uri: Optional[str] = None,
        state: Optional[str] = None,
        code_challenge: Optional[str] = None,
    ) -> Tuple[str, TwitterAuthState]:
        """Build authorization URL using OAuthLib WebApplicationClient."""
        
        scopes = scopes or [
            "tweet.read",
            "users.read",
        ]
        redirect = redirect_uri or (self.config.webhook_url or "")
        if not redirect:
            raise ValidationError("redirect_uri is required (use config.webhook_url or pass explicitly)")

        # Generate PKCE pair
        code_verifier, computed_challenge = generate_pkce_pair()
        challenge = code_challenge or computed_challenge
        session_state = state or base64.urlsafe_b64encode(secrets.token_bytes(16)).decode("utf-8").rstrip("=")

        # Use OAuthLib to prepare the authorization URL
        authorization_url = self.oauth_client.prepare_request_uri(
            AUTHORIZATION_URL,
            redirect_uri=redirect,
            scope=scopes,
            state=session_state,
            code_challenge=challenge,
            code_challenge_method="S256",
        )

        # Force scope to use %20 separators (not '+') to match X docs strictly
        try:
            parsed = urllib.parse.urlsplit(authorization_url)
            q = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
            query_dict: Dict[str, str] = {}
            for k, v in q:
                if k == "scope":
                    # Ensure scopes are joined by a single space
                    if isinstance(scopes, list):
                        v = " ".join(scopes)
                query_dict[k] = v
            new_query = urllib.parse.urlencode(query_dict, quote_via=urllib.parse.quote)
            authorization_url = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, new_query, parsed.fragment))
        except Exception:
            # If any issue occurs, fall back to the prepared URL
            pass

        auth_state = TwitterAuthState(
            code_verifier=code_verifier,
            code_challenge=challenge,
            state=session_state,
        )

        return authorization_url, auth_state

    def exchange_code_for_token(
        self,
        *,
        code: str,
        code_verifier: str,
        redirect_uri: Optional[str] = None,
    ) -> Dict[str, Any]:
        if DEMO_MODE:
            raise RuntimeError("External auth disabled in demo.")
        
        redirect = redirect_uri or (self.config.webhook_url or "")
        if not redirect:
            raise ValidationError("redirect_uri is required")

        # Prepare token data according to client type (public vs confidential)
        is_confidential = bool(self.config.client_secret and self.config.client_secret.get_secret_value())

        if is_confidential:
            # Confidential client: use Basic auth and do NOT include client_id in body per X docs
            token_data = {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect,
                "code_verifier": code_verifier,
            }
            basic = base64.b64encode(
                f"{self.config.client_id}:{self.config.client_secret.get_secret_value()}".encode("utf-8")
            ).decode("utf-8")
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
                "Authorization": f"Basic {basic}",
            }
        else:
            # Public client: include client_id in body
            token_data = {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect,
                "client_id": self.config.client_id,
                "code_verifier": code_verifier,
            }
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            }

        try:
            response = requests.post(
                TOKEN_URL,
                data=token_data,
                headers=headers,
                timeout=30
            )
            
            if response.status_code != 200:
                error_detail = response.text
                try:
                    error_json = response.json()
                    error_detail = f"{error_json.get('error', 'unknown')}: {error_json.get('error_description', 'no description')}"
                except:
                    pass
                raise APIRequestError(f"Token exchange failed (HTTP {response.status_code}): {error_detail}")

            # Parse the response
            token_response = response.json()
            
            # Extract tokens and update config
            self.config.access_token = SecretStr(token_response.get("access_token", ""))
            if "refresh_token" in token_response:
                self.config.refresh_token = SecretStr(token_response["refresh_token"])
            
            # Calculate expiry
            expires_in = token_response.get("expires_in")
            if expires_in:
                self.config.token_expires_at = datetime.now(timezone.utc) + timedelta(seconds=int(expires_in))

        except requests.RequestException as e:
            raise APIRequestError(f"Failed to exchange code for token: {str(e)}")

    def refresh_access_token(self) -> None:
        if DEMO_MODE:
            raise RuntimeError("External auth disabled in demo.")
        if not self.config.refresh_token:
            raise AuthenticationError("No refresh_token available")
        
        is_confidential = bool(self.config.client_secret and self.config.client_secret.get_secret_value())
        
        if is_confidential:
            # Confidential client
            data = {
                "grant_type": "refresh_token",
                "refresh_token": self.config.refresh_token.get_secret_value(),
            }
            basic = base64.b64encode(
                f"{self.config.client_id}:{self.config.client_secret.get_secret_value()}".encode("utf-8")
            ).decode("utf-8")
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Basic {basic}",
            }
        else:
            # Public client
            data = {
                "grant_type": "refresh_token",
                "client_id": self.config.client_id or "",
                "refresh_token": self.config.refresh_token.get_secret_value(),
            }
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            
        response = self.request_with_retry(
            "POST",
            TOKEN_URL,
            headers=headers,
            data=data,
            expected_statuses=[200],
        )
        payload = response.json()
        self._apply_token_payload(payload)

    def revoke_token(self, token: Optional[str] = None) -> None:
        """Revoke an access or refresh token."""
        if DEMO_MODE:
            raise RuntimeError("External auth disabled in demo.")
        token_to_revoke = token or (self.config.access_token.get_secret_value() if self.config.access_token else None)
        if not token_to_revoke:
            raise ValidationError("No token to revoke")
            
        is_confidential = bool(self.config.client_secret and self.config.client_secret.get_secret_value())
        
        if is_confidential:
            data = {"token": token_to_revoke}
            basic = base64.b64encode(
                f"{self.config.client_id}:{self.config.client_secret.get_secret_value()}".encode("utf-8")
            ).decode("utf-8")
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Basic {basic}",
            }
        else:
            data = {
                "token": token_to_revoke,
                "client_id": self.config.client_id,
            }
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            
        response = self.request_with_retry(
            "POST",
            REVOKE_URL,
            headers=headers,
            data=data,
            expected_statuses=[200],
        )

    def _apply_token_payload(self, payload: Dict[str, Any]) -> None:
        access_token = payload.get("access_token")
        refresh_token = payload.get("refresh_token")
        expires_in = payload.get("expires_in")  # seconds
        if not access_token:
            raise APIRequestError(f"Token response missing access_token: {payload}")

        self.config.access_token = SecretStr(access_token)
        if refresh_token:
            self.config.refresh_token = SecretStr(refresh_token)
        if expires_in:
            self.config.token_expires_at = datetime.now(timezone.utc) + timedelta(seconds=int(expires_in))

    # ---------- Core operations ----------
    def post_content(
        self,
        content_text: str,
        media_urls: Optional[List[str]] = None,
        scheduled_for: Optional[datetime] = None,
    ) -> PostResult:
        if DEMO_MODE:
            raise RuntimeError("Post operations disabled in demo.")
        if not self.config.access_token:
            raise AuthenticationError("Missing access_token for posting")
        if media_urls:
            # Media upload requires separate v1.1 flow. Not implemented in 4.1.
            raise ValidationError("media posting not supported yet")
        if scheduled_for is not None:
            # Native scheduling is not available via public API; would require external scheduler.
            raise ValidationError("scheduled posting not supported; provide None for scheduled_for")

        url = f"{self.config.api_base_url}/2/tweets"
        headers = {
            "Authorization": f"Bearer {self.config.access_token.get_secret_value()}",
            "Content-Type": "application/json",
        }
        body = {"text": content_text}
        response = self.request_with_retry(
            "POST", url, headers=headers, json=body, expected_statuses=[201, 200]
        )
        data = response.json()
        tweet_id = data.get("data", {}).get("id")
        if not tweet_id:
            raise APIRequestError(f"Unexpected tweet create response: {data}")
        return PostResult(
            platform=self.config.platform_name,
            post_id=tweet_id,
            url=f"https://redacted.example.com/{tweet_id}",
            raw_response=data,
        )

    def fetch_metrics(
        self,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        post_id: Optional[str] = None,
    ) -> List[MetricsResult]:
        if DEMO_MODE:
            return []
        if not self.config.access_token:
            raise AuthenticationError("Missing access_token for metrics")
        headers = {
            "Authorization": f"Bearer {self.config.access_token.get_secret_value()}",
        }

        results: List[MetricsResult] = []
        if post_id:
            url = f"{self.config.api_base_url}/2/tweets/{post_id}"
            params = {"tweet.fields": "public_metrics,organic_metrics,non_public_metrics,created_at"}
            resp = self.request_with_retry("GET", url, headers=headers, params=params, expected_statuses=[200])
            payload = resp.json()
            metrics = payload.get("data", {}).get("public_metrics", {})
            results.append(
                MetricsResult(
                    platform=self.config.platform_name,
                    post_id=post_id,
                    metrics=self.map_platform_metrics(metrics),
                    raw_response=payload,
                )
            )
            return results

        # Account-level recent tweets metrics window
        me_url = f"{self.config.api_base_url}/2/users/me"
        me_resp = self.request_with_retry("GET", me_url, headers=headers, expected_statuses=[200])
        me = me_resp.json().get("data", {})
        user_id = me.get("id")
        if not user_id:
            raise APIRequestError(f"Could not determine user id: {me_resp.text}")

        tweets_url = f"{self.config.api_base_url}/2/users/{user_id}/tweets"
        params: Dict[str, Any] = {
            "max_results": 100,
            "tweet.fields": "public_metrics,created_at",
        }
        if since:
            params["start_time"] = format_rfc3339(since)
        if until:
            params["end_time"] = format_rfc3339(until)

        resp = self.request_with_retry("GET", tweets_url, headers=headers, params=params, expected_statuses=[200])
        payload = resp.json()
        for item in payload.get("data", []) or []:
            metrics = item.get("public_metrics", {})
            results.append(
                MetricsResult(
                    platform=self.config.platform_name,
                    post_id=item.get("id"),
                    metrics=self.map_platform_metrics(metrics),
                    raw_response=item,
                )
            )
        return results

    # ---------- Connectivity ----------
    def test_connection(self) -> bool:
        if DEMO_MODE:
            return False
        if not self.config.access_token:
            return False
        headers = {
            "Authorization": f"Bearer {self.config.access_token.get_secret_value()}",
        }
        url = f"{self.config.api_base_url}/2/users/me"
        resp = self.request_with_retry("GET", url, headers=headers, expected_statuses=[200])
        return resp.status_code == 200


# Optional helper to register with the shared service (to be called in 4.3)

def register_with_service(service) -> None:
    service.register_platform("twitter", TwitterPlatform)
    service.register_platform("x", TwitterPlatform) 