from typing import Optional

# Registry of platform capabilities used by the orchestration layer
PLATFORM_CAPS = {
    "twitter": {"content": ["tweet"]},
    "shorts": {"content": ["shorts_caption", "video"]},
}


def insert_campaign(
    conn,
    name: str,
    description: str,
    persona_id: int,
    status: str = "draft",
) -> int:
    """Insert a campaign row and return its primary key.

    Note: Uses a fixed objective for now to keep this helper minimal. The
    orchestration determines the objective at a higher level.
    """
    cur = conn.cursor()
    campaign_id = cur.fetchone()[0]
    cur.close()
    return campaign_id


def update_campaign_core(
    conn,
    campaign_id: int,
    *,
    offer_id: Optional[int],
    persona_id: Optional[int],
    pain_point_id: Optional[int],
    primary_script_id: Optional[int],
    cta_link: Optional[str],
) -> None:
    """Update core campaign relationships/fields.

    This function does not commit. The caller controls the transaction.
    """
    cur = conn.cursor()
    cur.close()


def attach_platform(conn, campaign_id: int, platform: str, enabled: bool = True) -> None:
    """Attach a platform to the campaign if not already attached.

    Idempotent via ON CONFLICT DO NOTHING on the unique constraint.
    """
    cur = conn.cursor()
    cur.close()


# Content helpers

def create_content_tweet(
    conn,
    campaign_id: int,
    text: str,
    source: str = "ai",
) -> int:
    """Create a tweet draft content row for the given campaign and return id."""
    cur = conn.cursor()
    content_id = cur.fetchone()[0]
    cur.close()
    return content_id


def create_content_shorts_caption(
    conn,
    campaign_id: int,
    text: str,
    source: str = "ai",
) -> int:
    """Create a Shorts caption content row and return id."""
    cur = conn.cursor()
    content_id = cur.fetchone()[0]
    cur.close()
    return content_id


def create_pending_heygen_asset(conn, campaign_id: int, note: str) -> int:
    """Create a placeholder asset row compliant with current assets schema and return asset_id.

    Note: Current schema requires (type, url). We'll store a descriptive note and a dummy url.
    Prefer deferring real asset creation to webhook completion where we have the real URL.
    """
    cur = conn.cursor()
    asset_id = cur.fetchone()[0]
    cur.close()
    return asset_id


def create_content_shorts_video(
    conn,
    campaign_id: int,
    asset_id: Optional[int],
    source: str = "ai",
) -> int:
    """Create a Shorts video content row referencing an optional asset and return id."""
    cur = conn.cursor()

    content_id = cur.fetchone()[0]
    cur.close()
    return content_id 