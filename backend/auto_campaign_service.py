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
    
    
    This function now returns a mock ID for API compatibility.
    """
    
    return 1  # Mock campaign ID

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
    
    
    """
    
    pass

def attach_platform(conn, campaign_id: int, platform: str, enabled: bool = True) -> None:
    """Attach a platform to the campaign if not already attached.
    
    
    """
# Content helpers

def create_content_tweet(
    conn,
    campaign_id: int,
    text: str,
    source: str = "ai",
) -> int:
    """Create a tweet draft content row for the given campaign and return id.
    
    
    """
    
    return 1  # Mock content ID

def create_content_shorts_caption(
    conn,
    campaign_id: int,
    text: str,
    source: str = "ai",
) -> int:
    """Create a Shorts caption content row and return id.
    
    
    """
    
    return 1  # Mock content ID

def create_pending_heygen_asset(conn, campaign_id: int, note: str) -> int:
    """Create a placeholder asset row compliant with current assets schema and return asset_id.
    
    
    """
    
    return 1  # Mock asset ID

def create_content_shorts_video(
    conn,
    campaign_id: int,
    asset_id: Optional[int],
    source: str = "ai",
) -> int:
    """Create a Shorts video content row referencing an optional asset and return id.
    
    
    """
    
    return 1  # Mock content ID
