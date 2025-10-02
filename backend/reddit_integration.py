from __future__ import annotations

import json
import os
import secrets
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import praw
import prawcore
from pydantic import SecretStr
from demo_flags import DEMO_MODE

from social_media_service import (
    SocialMediaPlatform,
    PlatformConfig,
    PostResult,
    MetricsResult,
    ValidationError,
    AuthenticationError,
    AuthorizationError,
    RateLimitError,
    APIRequestError,
)


# Token storage file path
TOKEN_FILE = os.path.join(os.path.dirname(__file__), "reddit_token.json")


class RedditPlatform(SocialMediaPlatform):
    """Reddit integration using PRAW with refresh token authentication."""

    def __init__(self, config: PlatformConfig) -> None:
        super().__init__(config)
        self._reddit_client: Optional[praw.Reddit] = None
        self._user_agent = config.extra.get("user_agent", "redacted-app:v1.0 (by u/redacted)")

    def authenticate(self) -> None:
        """Initialize PRAW client with stored refresh token."""
        if DEMO_MODE:
            raise RuntimeError("External auth disabled in demo.")
        if not self.config.client_id:
            raise AuthenticationError("Missing Reddit client_id")
        if not self.config.client_secret:
            raise AuthenticationError("Missing Reddit client_secret")
        
        # Load refresh token from file if not in config
        if not self.config.refresh_token:
            self._load_refresh_token()
        
        if not self.config.refresh_token:
            raise AuthenticationError(
                "No Reddit refresh token available. Please authorize Reddit first by calling /api/social/reddit/auth/url"
            )

        try:
            self._reddit_client = praw.Reddit(
                client_id=self.config.client_id,
                client_secret=self.config.client_secret.get_secret_value(),
                refresh_token=self.config.refresh_token.get_secret_value(),
                user_agent=self._user_agent,
            )
            # Test the connection
            self._reddit_client.user.me()
        except prawcore.exceptions.ResponseException as e:
            if e.response.status_code == 401:
                raise AuthenticationError("Invalid Reddit credentials or expired token")
            elif e.response.status_code == 403:
                raise AuthorizationError("Reddit authorization failed")
            else:
                raise APIRequestError(f"Reddit API error: {str(e)}")
        except Exception as e:
            raise APIRequestError(f"Failed to authenticate with Reddit: {str(e)}")

    def _load_refresh_token(self) -> None:
        """Load refresh token from local storage."""
        if os.path.exists(TOKEN_FILE):
            try:
                with open(TOKEN_FILE, "r") as f:
                    data = json.load(f)
                    if "refresh_token" in data:
                        self.config.refresh_token = SecretStr(data["refresh_token"])
            except Exception:
                pass

    def save_refresh_token(self, refresh_token: str) -> None:
        """Save refresh token to local storage."""
        data = {"refresh_token": refresh_token, "saved_at": datetime.now(timezone.utc).isoformat()}
        with open(TOKEN_FILE, "w") as f:
            json.dump(data, f)
        self.config.refresh_token = SecretStr(refresh_token)

    def build_authorization_url(
        self,
        *,
        scopes: Optional[List[str]] = None,
        redirect_uri: Optional[str] = None,
        state: Optional[str] = None,
    ) -> Tuple[str, str]:
        """Build Reddit authorization URL for OAuth flow."""
        scopes = scopes or ["identity", "read", "submit"]
        redirect_uri = redirect_uri or self.config.extra.get("redirect_uri", "")
        if not redirect_uri:
            raise ValidationError("redirect_uri is required")
        
        state = state or base64.urlsafe_b64encode(secrets.token_bytes(16)).decode("utf-8").rstrip("=")
        
        # Create temporary Reddit instance for auth URL generation
        reddit = praw.Reddit(
            client_id=self.config.client_id,
            client_secret=self.config.client_secret.get_secret_value() if self.config.client_secret else None,
            redirect_uri=redirect_uri,
            user_agent=self._user_agent,
        )
        
        auth_url = reddit.auth.url(scopes=scopes, state=state, duration="permanent")
        return auth_url, state

    def exchange_code_for_token(self, *, code: str, redirect_uri: Optional[str] = None) -> str:
        """Exchange authorization code for refresh token."""
        if DEMO_MODE:
            raise APIRequestError("Token exchange disabled in demo mode.")
        redirect_uri = redirect_uri or self.config.extra.get("redirect_uri", "")
        if not redirect_uri:
            raise ValidationError("redirect_uri is required")
        
        reddit = praw.Reddit(
            client_id=self.config.client_id,
            client_secret=self.config.client_secret.get_secret_value() if self.config.client_secret else None,
            redirect_uri=redirect_uri,
            user_agent=self._user_agent,
        )
        refresh_token = reddit.auth.authorize(code)
        self.save_refresh_token(refresh_token)
        return refresh_token

    def post_content(
        self,
        content_text: str,
        media_urls: Optional[List[str]] = None,
        scheduled_for: Optional[datetime] = None,
    ) -> PostResult:
        """Submit a post to Reddit. Note: scheduled_for is not supported."""
        if DEMO_MODE:
            raise RuntimeError("Post operations disabled in demo.")
        if scheduled_for:
            raise ValidationError("Reddit does not support scheduled posts via API")
        if media_urls:
            raise ValidationError("Media uploads not yet implemented for Reddit")

        if not self._reddit_client:
            self.authenticate()

        # Extract subreddit and title from content_text
        # Expected format: "subreddit:SUBREDDIT_NAME title:TITLE text:CONTENT" or "url:URL"
        parts = content_text.split(" ", 3)
        subreddit_name = None
        title = None
        text = None
        url = None

        # Parse the input
        i = 0
        while i < len(parts):
            if parts[i].startswith("subreddit:"):
                subreddit_name = parts[i][10:]
                i += 1
            elif parts[i].startswith("title:"):
                # Title might have spaces, so we need to find where it ends
                title_start = i
                i += 1
                while i < len(parts) and not parts[i].startswith(("text:", "url:")):
                    i += 1
                title = " ".join(parts[title_start:i])[6:]  # Remove "title:" prefix
            elif parts[i].startswith("text:"):
                text = " ".join(parts[i:])[5:]  # Everything after "text:"
                break
            elif parts[i].startswith("url:"):
                url = " ".join(parts[i:])[4:]  # Everything after "url:"
                break
            else:
                i += 1

        if not subreddit_name or not title:
            raise ValidationError(
                "Invalid format. Use: 'subreddit:NAME title:TITLE text:CONTENT' or 'subreddit:NAME title:TITLE url:URL'"
            )

        try:
            subreddit = self._reddit_client.subreddit(subreddit_name)
            
            if url:
                # Link post
                submission = subreddit.submit(title=title, url=url)
            else:
                # Text post
                submission = subreddit.submit(title=title, selftext=text or "")
            
            return PostResult(
                platform=self.config.platform_name,
                post_id=submission.id,
                url=f"https://redacted.example.com{submission.permalink}",
                raw_response={"id": submission.id, "name": submission.name, "permalink": submission.permalink},
            )
        except prawcore.exceptions.Forbidden:
            raise AuthorizationError(f"Not allowed to post in r/{subreddit_name}")
        except prawcore.exceptions.NotFound:
            raise ValidationError(f"Subreddit r/{subreddit_name} not found")
        except Exception as e:
            raise APIRequestError(f"Failed to submit post: {str(e)}")

    def list_posts(self, subreddit: str, sort: str = "hot", limit: int = 25) -> List[Dict[str, Any]]:
        """List posts from a subreddit."""
        if DEMO_MODE:
            return []
        if not self._reddit_client:
            self.authenticate()

        try:
            sub = self._reddit_client.subreddit(subreddit)
            
            if sort == "hot":
                posts = sub.hot(limit=limit)
            elif sort == "new":
                posts = sub.new(limit=limit)
            elif sort == "top":
                posts = sub.top(limit=limit)
            elif sort == "rising":
                posts = sub.rising(limit=limit)
            else:
                raise ValidationError(f"Invalid sort option: {sort}. Use hot, new, top, or rising")

            results = []
            for post in posts:
                results.append({
                    "id": post.id,
                    "fullname": post.name,
                    "title": post.title,
                    "author": str(post.author) if post.author else "[deleted]",
                    "subreddit": post.subreddit.display_name,
                    "permalink": f"https://redacted.example.com{post.permalink}",
                    "score": post.score,
                    "num_comments": post.num_comments,
                    "created_utc": post.created_utc,
                    "is_self": post.is_self,
                    "url": post.url if not post.is_self else None,
                    "selftext": post.selftext if post.is_self else None,
                })
            return results
        except prawcore.exceptions.NotFound:
            raise ValidationError(f"Subreddit r/{subreddit} not found")
        except prawcore.exceptions.Forbidden:
            raise AuthorizationError(f"Access denied to r/{subreddit}")
        except Exception as e:
            raise APIRequestError(f"Failed to list posts: {str(e)}")

    def reply_to_post(self, submission_id: str, body: str) -> Dict[str, Any]:
        """Reply to a Reddit post."""
        if DEMO_MODE:
            raise RuntimeError("Post operations disabled in demo.")
        if not self._reddit_client:
            self.authenticate()

        try:
            submission = self._reddit_client.submission(id=submission_id)
            comment = submission.reply(body)
            return {
                "id": comment.id,
                "fullname": comment.name,
                "permalink": f"https://redacted.example.com{comment.permalink}",
                "body": comment.body,
            }
        except prawcore.exceptions.NotFound:
            raise ValidationError(f"Post {submission_id} not found")
        except prawcore.exceptions.Forbidden:
            raise AuthorizationError("Not allowed to comment on this post")
        except Exception as e:
            raise APIRequestError(f"Failed to reply to post: {str(e)}")

    def reply_to_comment(self, comment_id: str, body: str) -> Dict[str, Any]:
        """Reply to a Reddit comment."""
        if DEMO_MODE:
            raise RuntimeError("Post operations disabled in demo.")
        if not self._reddit_client:
            self.authenticate()

        try:
            comment = self._reddit_client.comment(id=comment_id)
            reply = comment.reply(body)
            return {
                "id": reply.id,
                "fullname": reply.name,
                "permalink": f"https://redacted.example.com{reply.permalink}",
                "body": reply.body,
            }
        except prawcore.exceptions.NotFound:
            raise ValidationError(f"Comment {comment_id} not found")
        except prawcore.exceptions.Forbidden:
            raise AuthorizationError("Not allowed to reply to this comment")
        except Exception as e:
            raise APIRequestError(f"Failed to reply to comment: {str(e)}")

    def fetch_metrics(
        self,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        post_id: Optional[str] = None,
    ) -> List[MetricsResult]:
        """Fetch metrics for posts. Note: Reddit doesn't provide historical metrics."""
        if DEMO_MODE:
            return []
        if not self._reddit_client:
            self.authenticate()

        results = []
        
        if post_id:
            try:
                submission = self._reddit_client.submission(id=post_id)
                submission._fetch()  # Ensure we have the latest data
                
                metrics = {
                    "score": submission.score,
                    "upvote_ratio": submission.upvote_ratio,
                    "num_comments": submission.num_comments,
                    "num_crossposts": submission.num_crossposts,
                    "total_awards_received": submission.total_awards_received,
                }
                
                results.append(
                    MetricsResult(
                        platform=self.config.platform_name,
                        post_id=post_id,
                        metrics=metrics,
                        raw_response=metrics,
                    )
                )
            except Exception as e:
                raise APIRequestError(f"Failed to fetch metrics for post {post_id}: {str(e)}")
        else:
            # Reddit doesn't support fetching metrics for a time range
            # We could fetch recent posts and their current metrics
            raise ValidationError("Reddit does not support fetching metrics by time range")

        return results

    def test_connection(self) -> bool:
        """Test Reddit connection by getting current user."""
        if DEMO_MODE:
            return False
        try:
            if not self._reddit_client:
                self.authenticate()
            user = self._reddit_client.user.me()
            return user is not None
        except Exception:
            return False


def register_with_service(service) -> None:
    """Register Reddit platform with the social media service."""
    service.register_platform("reddit", RedditPlatform)


# Import base64 for state generation
import base64 