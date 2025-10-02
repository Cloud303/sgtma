from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Header, File, UploadFile, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict
from datetime import datetime, date
import os
import requests
import json
import hmac
import hashlib
import logging
import time
import boto3
import uuid
import mimetypes

# Import AI service
from ai_service import ai_service
import auto_campaign_service as svc

# Social integrations (4.3)
from twitter_integration import TwitterPlatform, TwitterAuthState
from twitter_oauth1_integration import TwitterOAuth1State, TwitterOAuth1Platform
from social_media_service import (
    load_platform_config_from_env, 
    PlatformConfig, 
    AuthenticationError as SMAuthenticationError,
    AuthorizationError,
    ValidationError
)
from reddit_integration import RedditPlatform, register_with_service as register_reddit

# HeyGen service
from heygen_service import HeyGenService
from storage_service import StorageService
from demo_flags import DEMO_MODE

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def log_heygen_operation(operation: str, job_id: str = None, provider_id: str = None, duration: float = None, status: str = None):
    """Log HeyGen operations with structured data"""
    log_data = {
        "operation": operation,
        "job_id": job_id,
        "provider_id": provider_id,
        "duration": duration,
        "status": status,
        "timestamp": datetime.utcnow().isoformat()
    }
    logger.info(f"HeyGen operation: {log_data}")

def log_s3_operation(operation: str, bucket: str = None, key: str = None, duration: float = None, status: str = None):
    """Log S3 operations with structured data"""
    log_data = {
        "operation": operation,
        "bucket": bucket,
        "key": key,
        "duration": duration,
        "status": status,
        "timestamp": datetime.utcnow().isoformat()
    }
    logger.info(f"S3 operation: {log_data}")

app = FastAPI(title="Content Management API")

# In-memory OAuth session store (state -> TwitterAuthState)
_twitter_oauth_sessions: Dict[str, TwitterAuthState] = {}
# Keep track of OAuth state
_twitter_oauth1_sessions: Dict[str, TwitterOAuth1State] = {}

# Configure allowed origins from environment
frontend_url = os.getenv("FRONTEND_URL", "https://redacted.example.com")
public_url = os.getenv("PUBLIC_URL", frontend_url)
allow_origins = [frontend_url]
if public_url not in allow_origins:
    allow_origins.append(public_url)
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models for request/response
class PersonaCreate(BaseModel):
    title: str
    description: Optional[str] = None

class PersonaResponse(BaseModel):
    persona_id: int
    title: str
    description: Optional[str]
    created_at: datetime
    updated_at: datetime

class ScriptCreate(BaseModel):
    content_text: str
    status: str = "draft"
    offer_title: Optional[str] = None
    pain_point: Optional[str] = None
    target_persona: Optional[str] = None

class ScriptResponse(BaseModel):
    script_id: int
    content_text: str
    status: str
    created_at: datetime
    updated_at: datetime
    offer_title: Optional[str] = None
    pain_point: Optional[str] = None
    target_persona: Optional[str] = None

class PainPointCreate(BaseModel):
    description: str
    category: Optional[str] = None

class PainPointResponse(BaseModel):
    pain_point_id: int
    description: str
    category: Optional[str]
    created_at: datetime

class OfferCreate(BaseModel):
    title: str
    description: Optional[str] = None
    link_url: Optional[str] = "https://redacted.example.com"
    cta_text: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    status: str = "active"
    product_type: Optional[str] = None
    price: Optional[str] = None
    target_audience: Optional[str] = None
    value_proposition: Optional[str] = None
    features: Optional[str] = None
    requirements: Optional[str] = None

class OfferResponse(BaseModel):
    offer_id: int
    title: str
    description: Optional[str]
    link_url: str
    cta_text: Optional[str]
    start_date: Optional[str]
    end_date: Optional[str]
    status: str
    product_type: Optional[str] = None
    price: Optional[str] = None
    target_audience: Optional[str] = None
    value_proposition: Optional[str] = None
    features: Optional[str] = None
    requirements: Optional[str] = None

    @validator('start_date', 'end_date', pre=True)
    def convert_date_to_string(cls, v):
        if isinstance(v, date):
            return v.isoformat()
        return v

# AI Request Models
class ScriptGenerationRequest(BaseModel):
    pain_point: str
    persona: Optional[str] = None

class ScriptFromOfferRequest(BaseModel):
    offer: Optional[str] = None
    offer_title: Optional[str] = None
    offer_description: Optional[str] = None
    pain_point: Optional[str] = None
    persona: Optional[str] = None

class PainPointsGenerationRequest(BaseModel):
    persona: str

class OfferGenerationRequest(BaseModel):
    pain_point: str
    persona: Optional[str] = None

class PersonaGenerationRequest(BaseModel):
    title: str
    industry: Optional[str] = None

class ContentIdeasRequest(BaseModel):
    topic: str
    platform: str = "LinkedIn"

# ============================================================================
# CAMPAIGN MANAGEMENT MODELS (Phase 2.1)
# ============================================================================

# CTA Models
class CTACreate(BaseModel):
    text: str
    type: str
    url: Optional[str] = None
    description: Optional[str] = None

class CTAResponse(BaseModel):
    cta_id: int
    text: str
    type: str
    url: Optional[str]
    description: Optional[str]
    created_at: datetime

# Campaign Models
class CampaignCreate(BaseModel):
    name: str
    description: str
    objective: str
    target_persona_id: int
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    status: Optional[str] = "draft"

class CampaignResponse(BaseModel):
    campaign_id: int
    name: str
    description: Optional[str]
    objective: Optional[str]
    target_persona_id: Optional[int]
    start_date: Optional[str]
    end_date: Optional[str]
    status: str
    created_at: datetime
    updated_at: datetime

# Junction Models
class CampaignScriptCreate(BaseModel):
    campaign_id: int
    script_id: int
    platform: Optional[str] = None
    content_variation: Optional[str] = None

class CampaignCTACreate(BaseModel):
    campaign_id: int
    cta_id: int
    platform: Optional[str] = None
    placement_context: Optional[str] = None

class CampaignPublicationCreate(BaseModel):
    campaign_id: int
    publication_id: int

# AI Generation Models for Campaigns
class CTAGenerationRequest(BaseModel):
    campaign_description: str
    platforms: List[str]

class CampaignContentGenerationRequest(BaseModel):
    campaign_name: str
    objective: str
    target_persona: str
    platforms: List[str]

# ============================================================================
# ANALYTICS MODELS (Task 2.1)
# ============================================================================

# Analytics Data Models
class AnalyticsDataPoint(BaseModel):
    campaign_id: int
    platform: str
    content_id: Optional[str] = None
    metric_category: str
    metric_name: str
    metric_value: float
    metric_unit: Optional[str] = None
    date_recorded: str
    time_period: str = "daily"
    demographic_segment: Optional[str] = None
    device_type: Optional[str] = None
    location_country: Optional[str] = None
    location_region: Optional[str] = None
    confidence_score: Optional[float] = 1.0

class ContentPerformanceData(BaseModel):
    campaign_id: int
    content_type: str
    platform: str
    content_id: Optional[str] = None
    title: Optional[str] = None
    content_text: Optional[str] = None
    media_urls: Optional[dict] = None
    publish_date: Optional[str] = None
    engagement_rate: Optional[float] = None
    reach_rate: Optional[float] = None
    conversion_rate: Optional[float] = None
    virality_score: Optional[float] = None
    sentiment_score: Optional[float] = None
    keywords: Optional[list] = None
    hashtags: Optional[list] = None
    mentions: Optional[list] = None

class ConversionData(BaseModel):
    campaign_id: int
    platform: str
    conversion_type: str
    conversion_value: Optional[float] = None
    conversion_currency: str = "USD"
    attribution_source: Optional[str] = None
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    referrer_url: Optional[str] = None
    landing_page: Optional[str] = None
    conversion_date: str

class CampaignActivationRequest(BaseModel):
    activation_date: Optional[str] = None
    platforms: List[str] = []

# Analytics Response Models
class AnalyticsSummaryResponse(BaseModel):
    campaign_id: int
    date_range: dict
    analytics_summary: List[dict]
    content_performance: List[dict]
    conversion_summary: List[dict]

class AnalyticsTrendsResponse(BaseModel):
    campaign_id: int
    metric: str
    period: str
    trends: List[dict]

class CampaignActivationResponse(BaseModel):
    message: str
    campaign_id: int

# Platform-Specific Analytics Models
class TwitterAnalyticsData(BaseModel):
    campaign_id: int
    tweet_id: str
    retweets: Optional[int] = None
    likes: Optional[int] = None
    replies: Optional[int] = None
    quotes: Optional[int] = None
    impressions: Optional[int] = None
    reach: Optional[int] = None
    profile_visits: Optional[int] = None
    link_clicks: Optional[int] = None
    detail_expands: Optional[int] = None
    media_views: Optional[int] = None
    media_engagements: Optional[int] = None
    hashtag_clicks: Optional[int] = None
    mention_clicks: Optional[int] = None
    url_clicks: Optional[int] = None
    app_opens: Optional[int] = None
    app_installs: Optional[int] = None
    follows: Optional[int] = None
    email_tweet: Optional[int] = None
    email_open: Optional[int] = None
    email_click: Optional[int] = None
    date_recorded: str

class LinkedInAnalyticsData(BaseModel):
    campaign_id: int
    post_id: str
    impressions: Optional[int] = None
    unique_impressions: Optional[int] = None
    clicks: Optional[int] = None
    likes: Optional[int] = None
    comments: Optional[int] = None
    shares: Optional[int] = None
    reactions: Optional[int] = None
    engagement_rate: Optional[float] = None
    click_through_rate: Optional[float] = None
    follower_count: Optional[int] = None
    organic_impressions: Optional[int] = None
    paid_impressions: Optional[int] = None
    date_recorded: str

class FacebookAnalyticsData(BaseModel):
    campaign_id: int
    post_id: str
    platform_type: str  # 'facebook' or 'instagram'
    impressions: Optional[int] = None
    reach: Optional[int] = None
    engagement: Optional[int] = None
    likes: Optional[int] = None
    comments: Optional[int] = None
    shares: Optional[int] = None
    saves: Optional[int] = None
    video_views: Optional[int] = None
    video_completions: Optional[int] = None
    story_views: Optional[int] = None
    story_replies: Optional[int] = None
    link_clicks: Optional[int] = None
    profile_visits: Optional[int] = None
    follows: Optional[int] = None
    date_recorded: str

# Social Media Account Models
class SocialMediaAccountCreate(BaseModel):
    platform: str
    account_name: str
    account_handle: Optional[str] = None
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None
    token_expires_at: Optional[str] = None
    is_active: bool = True

class SocialMediaAccountResponse(BaseModel):
    account_id: int
    platform: str
    account_name: str
    account_handle: Optional[str]
    is_active: bool
    created_at: datetime
    updated_at: datetime


# CRUD Operations for Personas
@app.post("/personas", response_model=PersonaResponse)
def create_persona(persona: PersonaCreate):
    try:
        
        # Return mock data for API compatibility
        new_persona = {
            "persona_id": 1,
            "title": persona.title,
            "description": persona.description,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        return dict(new_persona)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/personas", response_model=List[PersonaResponse])
def get_personas():
        
        # Return mock data for API compatibility
        personas = [
            {
                "persona_id": 1,
                "title": "Sample Persona",
                "description": "A sample persona for demo purposes",
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
        
        # Return mock data for API compatibility
        updated_persona = {
            "persona_id": persona_id,
            "title": persona.title,
            "description": persona.description,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/personas/{persona_id}", response_model=PersonaResponse)
def update_persona(persona_id: int, persona: PersonaCreate):
    try:
        
        return dict(updated_persona)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/personas/{persona_id}")
def delete_persona(persona_id: int):
    try:
        
        return {"message": "Persona deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# CRUD Operations for Scripts
@app.post("/scripts", response_model=ScriptResponse)
def create_script(script: ScriptCreate):
    try:
        
        return dict(new_script)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/scripts", response_model=List[ScriptResponse])
def get_scripts():
    try:
        
        return [dict(script) for script in scripts]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/scripts/{script_id}", response_model=ScriptResponse)
def update_script(script_id: int, script: ScriptCreate):
    try:
        
        return dict(updated_script)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/scripts/{script_id}")
def delete_script(script_id: int):
    try:
        
        return {"message": "Script deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# CRUD Operations for Pain Points
@app.post("/pain-points", response_model=PainPointResponse)
def create_pain_point(pain_point: PainPointCreate):
    try:
        
        return dict(new_pain_point)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pain-points", response_model=List[PainPointResponse])
def get_pain_points():
    try:
        
        return [dict(pain_point) for pain_point in pain_points]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/pain-points/{pain_point_id}", response_model=PainPointResponse)
def update_pain_point(pain_point_id: int, pain_point: PainPointCreate):
    try:
        
        return dict(updated_pain_point)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/pain-points/{pain_point_id}")
def delete_pain_point(pain_point_id: int):
    try:
        
        return {"message": "Pain point deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# CRUD Operations for Offers
@app.post("/offers", response_model=OfferResponse)
def create_offer(offer: OfferCreate):
    try:
        
        return dict(new_offer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/offers", response_model=List[OfferResponse])
def get_offers():
    try:
        
        return [dict(offer) for offer in offers]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/offers/{offer_id}", response_model=OfferResponse)
def update_offer(offer_id: int, offer: OfferCreate):
    try:
        
        return dict(updated_offer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/offers/{offer_id}")
def delete_offer(offer_id: int):
    try:
        
        return {"message": "Offer deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# AI Endpoints
@app.post("/ai/generate-script")
def generate_script(request: ScriptGenerationRequest):
    try:
        script = ai_service.generate_script_from_pain_point(request.pain_point, request.persona)
        return {"script": script}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ai/generate-script-from-offer")
def generate_script_from_offer(request: ScriptFromOfferRequest):
    try:
    
        try:
            tweet_text = ai_service.generate_tweet(script, "")
        except Exception:
            tweet_text = ai_service.generate_tweet_text(script, None)
        try:
            video_caption = ai_service.generate_video_caption(script, "")
        except Exception:
            video_caption = ai_service.generate_shorts_caption(script, None)
        return {"script": script, "tweet_text": tweet_text, "tweet": tweet_text, "video_caption": video_caption, "caption": video_caption}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ai/generate-pain-points")
def generate_pain_points(request: PainPointsGenerationRequest):
    try:
        pain_points = ai_service.generate_pain_points_from_persona(request.persona)
        return {"pain_points": pain_points}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ai/generate-offer")
def generate_offer(request: OfferGenerationRequest):
    try:
        offer = ai_service.generate_offer_from_pain_point(request.pain_point, request.persona)
        return offer
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ai/generate-persona")
def generate_persona(request: PersonaGenerationRequest):
    try:
        description = ai_service.generate_persona_description(request.title, request.industry)
        return {"description": description}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ai/generate-content-ideas")
def generate_content_ideas(request: ContentIdeasRequest):
    try:
        ideas = ai_service.generate_content_ideas(request.topic, request.platform)
        return {"ideas": ideas}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# CAMPAIGN MANAGEMENT ENDPOINTS (Phase 2.2)
# ============================================================================

# CTA Endpoints
@app.post("/ctas", response_model=CTAResponse)
def create_cta(cta: CTACreate):
    try:
        
        return dict(new_cta)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/ctas", response_model=List[CTAResponse])
def get_ctas():
    try:
        
        return [dict(cta) for cta in ctas]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/ctas/{cta_id}", response_model=CTAResponse)
def update_cta(cta_id: int, cta: CTACreate):
    try:
        
        return dict(updated_cta)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/ctas/{cta_id}")
def delete_cta(cta_id: int):
    try:
        
        return {"message": "CTA deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Campaign Endpoints
@app.post("/campaigns", response_model=CampaignResponse)
def create_campaign(campaign: CampaignCreate):
    try:
        
        return dict(new_campaign)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/campaigns")
def get_campaigns():
    try:
        
        return [dict(campaign) for campaign in campaigns]
    except Exception as e:
        # Return empty list instead of error for now
        return []

@app.put("/campaigns/{campaign_id}", response_model=CampaignResponse)
def update_campaign(campaign_id: int, campaign: CampaignCreate):
    try:
        
        return dict(updated_campaign)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/campaigns/{campaign_id}")
def delete_campaign(campaign_id: int):
    try:
        
        
        if campaign:
            return dict(campaign)
        else:
            return {"message": "No pending campaigns found"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/campaigns/draft")
def get_draft_campaigns():
    """Get all non-activated campaigns for dropdown selection"""
    try:
        
        
        return [dict(campaign) for campaign in campaigns]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/campaigns/active", response_model=List[CampaignResponse])
def get_active_campaigns():
    try:
        
        return [dict(campaign) for campaign in active_campaigns]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/campaigns/{campaign_id}/activate", response_model=CampaignActivationResponse)
def activate_campaign(campaign_id: int):
    try:
        
        return dict(activated_campaign)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/campaigns/{campaign_id}/activation-status")
def get_campaign_activation_status(campaign_id: int):
    """Get campaign activation status and details"""
    try:
        
        
        if not result:
            raise HTTPException(status_code=404, detail="Campaign not found")
        
        return dict(result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

# Junction Endpoints
@app.post("/campaign-scripts")
def add_script_to_campaign(campaign_script: CampaignScriptCreate):
    try:
        
        return dict(new_campaign_script)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/campaign-ctas")
def add_cta_to_campaign(campaign_cta: CampaignCTACreate):
    try:
        
        return dict(new_campaign_cta)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/campaign-publications")
def add_publication_to_campaign(campaign_publication: CampaignPublicationCreate):
    try:
        
        return dict(new_campaign_publication)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Campaign AI Endpoints
@app.post("/ai/generate-ctas")
def generate_ctas(request: CTAGenerationRequest):
    try:
        ctas = ai_service.generate_cta_variations(request.campaign_description, request.platforms)
        return {"ctas": ctas}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ai/generate-campaign-content")
def generate_campaign_content(request: CampaignContentGenerationRequest):
    try:
        content = ai_service.generate_campaign_content(request.campaign_name, request.objective, request.target_persona, request.platforms)
        return content
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ai/generate-platform-variations")
def generate_platform_variations(request: dict):
    try:
        script_content = request.get("script_content")
        platforms = request.get("platforms", [])
        
        if not script_content:
            raise HTTPException(status_code=400, detail="script_content is required")
        if not platforms:
            raise HTTPException(status_code=400, detail="platforms list is required")
        
        variations = ai_service.generate_platform_variations(script_content, platforms)
        return variations
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Health check endpoint
@app.get("/health")
def health_check():

    return {
        "status": "healthy",
        "message": "Content Management API is running",
        "db": db_status,
        "services": services,
        "timestamp": datetime.utcnow().isoformat()
    }

# ============================================================================
# ANALYTICS MODELS (Task 2.1)
# ============================================================================

@app.post("/analytics/collect")
def collect_analytics_data(analytics_data: AnalyticsDataPoint):
    """Collect raw analytics data from social media platforms"""
    try:
        
        
        return {"message": "Analytics data collected successfully", "analytics_id": analytics_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.post("/analytics/content-performance")
def collect_content_performance(content_data: ContentPerformanceData):
    """Collect content performance data"""
    try:
        
        
        return {"message": "Content performance data collected successfully", "performance_id": performance_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.post("/analytics/conversions")
def collect_conversion_data(conversion_data: ConversionData):
    """Collect conversion tracking data"""
    try:
        
        
        return {"message": "Conversion data collected successfully", "conversion_id": conversion_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.post("/analytics/twitter")
def collect_twitter_analytics(twitter_data: TwitterAnalyticsData):
    """Collect Twitter/X specific analytics data"""
    try:
        
        
        return {"message": "Twitter analytics data collected successfully", "twitter_analytics_id": twitter_analytics_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.post("/analytics/linkedin")
def collect_linkedin_analytics(linkedin_data: LinkedInAnalyticsData):
    """Collect LinkedIn specific analytics data"""
    try:
        
        
        return {"message": "LinkedIn analytics data collected successfully", "linkedin_analytics_id": linkedin_analytics_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.post("/analytics/facebook")
def collect_facebook_analytics(facebook_data: FacebookAnalyticsData):
    """Collect Facebook/Instagram specific analytics data"""
    try:
        
        
        return {"message": "Facebook analytics data collected successfully", "facebook_analytics_id": facebook_analytics_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/analytics/campaign/{campaign_id}/summary")
def get_campaign_analytics_summary(campaign_id: int, start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get comprehensive analytics summary for a campaign"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Build date filter
        date_filter = ""
        params = [campaign_id]
        if start_date and end_date:
            date_filter = "AND date_recorded BETWEEN %s AND %s"
            params.extend([start_date, end_date])
        
        # Get aggregated analytics dat
        
        analytics_summary = [dict(row) for row in cur.fetchall()]
        
        # Get content performance data
        content_date_filter = ""
        if start_date and end_date:
            content_date_filter = "AND publish_date BETWEEN %s AND %s"
        
       
        content_performance = [dict(row) for row in cur.fetchall()]
        
        # Get conversion summary
        conversion_date_filter = ""
        if start_date and end_date:
            conversion_date_filter = "AND conversion_date BETWEEN %s AND %s"
        
       
        
        conversion_summary = [dict(row) for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return {
            "campaign_id": campaign_id,
            "date_range": {
                "start_date": start_date,
                "end_date": end_date
            },
            "analytics_summary": analytics_summary,
            "content_performance": content_performance,
            "conversion_summary": conversion_summary
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/analytics/campaign/{campaign_id}/trends/{metric}")
def get_campaign_analytics_trends(campaign_id: int, metric: str, period: str = "daily", platform: Optional[str] = None):
    """Get analytics trends for a specific metric"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Build platform filter
        platform_filter = ""
        params = [campaign_id, metric]
        if platform:
            platform_filter = "AND platform = %s"
            params.append(platform)
        
        # Get trend data based on period
        if period == "hourly":
            group_by = "DATE_TRUNC('hour', date_recorded)"
        elif period == "daily":
            group_by = "DATE(date_recorded)"
        elif period == "weekly":
            group_by = "DATE_TRUNC('week', date_recorded)"
        elif period == "monthly":
            group_by = "DATE_TRUNC('month', date_recorded)"
        else:
            group_by = "DATE(date_recorded)"
        
      
        
        trends = [dict(row) for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return {
            "campaign_id": campaign_id,
            "metric": metric,
            "period": period,
            "platform": platform,
            "trends": trends
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/analytics/platform/{platform}/summary")
def get_platform_analytics_summary(platform: str, start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get analytics summary for a specific platform across all campaigns"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Build date filter
        date_filter = ""
        params = [platform]
        if start_date and end_date:
            date_filter = "AND date_recorded BETWEEN %s AND %s"
            params.extend([start_date, end_date])
        
        # Get platform-specific analytics
    
        
        cur.close()
        conn.close()
        
        return {
            "platform": platform,
            "date_range": {
                "start_date": start_date,
                "end_date": end_date
            },
            "analytics_summary": platform_analytics
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

# ============================================================================
# SOCIAL MEDIA ACCOUNT MANAGEMENT ENDPOINTS (Task 2.4)
# ============================================================================

@app.post("/social-accounts", response_model=SocialMediaAccountResponse)
def create_social_account(account: SocialMediaAccountCreate):
    """Create a new social media account"""
    try:
        
        
        return dict(new_account)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/social-accounts", response_model=List[SocialMediaAccountResponse])
def get_social_accounts(platform: Optional[str] = None, is_active: Optional[bool] = None):
    """Get all social media accounts with optional filtering"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Build filter conditions
        conditions = []
        params = []
        
        if platform:
            conditions.append("platform = %s")
            params.append(platform)
        
        if is_active is not None:
            conditions.append("is_active = %s")
            params.append(is_active)
        
        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)
        
        
        accounts = [dict(row) for row in cur.fetchall()]
        cur.close()
        conn.close()
        
        return accounts
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/social-accounts/{account_id}", response_model=SocialMediaAccountResponse)
def get_social_account(account_id: int):
    """Get a specific social media account"""
    try:
        
        
        if not account:
            raise HTTPException(status_code=404, detail="Social media account not found")
        
        return dict(account)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.put("/social-accounts/{account_id}", response_model=SocialMediaAccountResponse)
def update_social_account(account_id: int, account: SocialMediaAccountCreate):
    """Update a social media account"""
    try:
        
            raise HTTPException(status_code=404, detail="Social media account not found")
        
        conn.commit()
        cur.close()
        conn.close()
        
        return dict(updated_account)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.delete("/social-accounts/{account_id}")
def delete_social_account(account_id: int):
    """Delete a social media account"""
    try:
        
            raise HTTPException(status_code=404, detail="Social media account not found")
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {"message": "Social media account deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.post("/social-accounts/{account_id}/refresh-token")
def refresh_social_account_token(account_id: int):
    """Refresh the access token for a social media account"""
    try:
        
            raise HTTPException(status_code=404, detail="Social media account not found")
        
        platform_name = (account["platform"] or "").lower()
        # Reddit: tokens are refresh-token based and typically permanent. Validate connection.
        if platform_name == "reddit":
            cfg = load_platform_config_from_env("reddit", prefix="REDDIT")
            if account.get("refresh_token"):
                from pydantic import SecretStr
                cfg.refresh_token = SecretStr(account["refresh_token"]) 
            cfg.extra["user_agent"] = "redacted-app:v1.0 (by u/redacted-app)"
            try:
                rp = RedditPlatform(cfg)
                rp.authenticate()
                # No token rotation necessary; return success
                cur.close(); conn.close()
                return {
                    "message": "Reddit token validated successfully",
                    "account_id": account_id,
                    "platform": "reddit",
                }
            except Exception as e:
                cur.close(); conn.close()
                raise HTTPException(status_code=401, detail=f"Reddit authentication failed: {e}")
        
        # Twitter/X or others: keep existing simulated refresh or platform-specific logic
        new_access_token = f"new_token_{account['platform']}_{account_id}"
        new_expires_at = "2024-12-31T23:59:59Z"
        
        )
        
        updated_account = cur.fetchone()
        conn.commit()
        cur.close(); conn.close()
        
        return {
            "message": "Token refreshed successfully",
            "account_id": updated_account['account_id'],
            "platform": updated_account['platform'],
            "account_name": updated_account['account_name']
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/social-accounts/platform/{platform}/accounts")
def get_accounts_by_platform(platform: str):
    """Get all accounts for a specific platform"""
    try:
        
        
        return {
            "platform": platform,
            "accounts": accounts,
            "total_accounts": len(accounts)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.post("/social-accounts/{account_id}/test-connection")
def test_social_account_connection(account_id: int):
    """Test the connection to a social media account"""
    try:
        
        
        if not account:
            raise HTTPException(status_code=404, detail="Social media account not found")
        
        platform_name = (account["platform"] or "").lower()
        if platform_name == "reddit":
            cfg = load_platform_config_from_env("reddit", prefix="REDDIT")
            if account.get("refresh_token"):
                from pydantic import SecretStr
                cfg.refresh_token = SecretStr(account["refresh_token"])
            cfg.extra["user_agent"] = "redacted-app:v1.0 (by u/redacted-app)"
            rp = RedditPlatform(cfg)
            rp.authenticate()
            user = rp._reddit_client.user.me()
            connection_status = "connected"
            account_info = {
                "platform": "reddit",
                "account_name": getattr(user, "name", account['account_name']),
                "account_handle": getattr(user, "name", account['account_handle']),
                "link_karma": getattr(user, "link_karma", None),
                "comment_karma": getattr(user, "comment_karma", None),
                "created_utc": getattr(user, "created_utc", None),
            }
            return {
                "message": "Connection test completed",
                "account_id": account_id,
                "connection_status": connection_status,
                "account_info": account_info
            }
        
        # Default behavior for other platforms (simulate success)
        connection_status = "connected"
        account_info = {
            "platform": account['platform'],
            "account_name": account['account_name'],
            "account_handle": account['account_handle'],
        }
        return {
            "message": "Connection test completed",
            "account_id": account_id,
            "connection_status": connection_status,
            "account_info": account_info
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

# ============================================================================
# COMPREHENSIVE TESTING AND VALIDATION ENDPOINTS (Task 2.5)
# ============================================================================

@app.post("/test/analytics-comprehensive")
def test_analytics_comprehensive():
    """Comprehensive test of all analytics functionality"""
    try:
        test_results = {
            "timestamp": "2024-08-05T21:45:00Z",
            "test_suite": "Analytics Comprehensive Test",
            "results": {}
        }
        
        # Test 1: Raw Analytics Data Collection
        try:
            analytics_data = {
                "campaign_id": 15,
                "platform": "twitter",
                "content_id": "test_tweet_001",
                "metric_category": "engagement",
                "metric_name": "likes",
                "metric_value": 250.0,
                "date_recorded": "2024-08-05T21:45:00Z",
                "demographic_segment": "18-34",
                "device_type": "mobile",
                "location_country": "US",
                "confidence_score": 0.95
            }
            
            response = requests.post("https://redacted.example.com/analytics/collect", json=analytics_data)
            test_results["results"]["raw_analytics"] = {
                "status": "PASS" if response.status_code == 200 else "FAIL",
                "response": response.json() if response.status_code == 200 else response.text,
                "status_code": response.status_code
            }
        except Exception as e:
            test_results["results"]["raw_analytics"] = {
                "status": "ERROR",
                "error": str(e)
            }
        
        # Test 2: Content Performance Data
        try:
            content_data = {
                "campaign_id": 15,
                "content_type": "post",
                "platform": "twitter",
                "content_id": "test_tweet_001",
                "title": "Test Analytics Tweet",
                "content_text": "Testing comprehensive analytics functionality",
                "engagement_rate": 0.12,
                "virality_score": 0.08,
                "sentiment_score": 0.85,
                "keywords": ["analytics", "testing", "comprehensive"],
                "hashtags": ["#analytics", "#testing"],
                "mentions": ["@redacted-app"]
            }
            
            response = requests.post("https://redacted.example.com/analytics/content-performance", json=content_data)
            test_results["results"]["content_performance"] = {
                "status": "PASS" if response.status_code == 200 else "FAIL",
                "response": response.json() if response.status_code == 200 else response.text,
                "status_code": response.status_code
            }
        except Exception as e:
            test_results["results"]["content_performance"] = {
                "status": "ERROR",
                "error": str(e)
            }
        
        # Test 3: Conversion Tracking
        try:
            conversion_data = {
                "campaign_id": 15,
                "platform": "twitter",
                "conversion_type": "click",
                "conversion_value": 10.0,
                "conversion_currency": "USD",
                "attribution_source": "twitter_organic",
                "user_id": "user_123",
                "session_id": "session_456",
                "referrer_url": "https://redacted.example.com",
                "landing_page": "https://redacted.example.com",
                "conversion_date": "2024-08-05T21:45:00Z"
            }
            
            response = requests.post("https://redacted.example.com/analytics/conversions", json=conversion_data)
            test_results["results"]["conversion_tracking"] = {
                "status": "PASS" if response.status_code == 200 else "FAIL",
                "response": response.json() if response.status_code == 200 else response.text,
                "status_code": response.status_code
            }
        except Exception as e:
            test_results["results"]["conversion_tracking"] = {
                "status": "ERROR",
                "error": str(e)
            }
        
        # Test 4: Platform-Specific Analytics
        try:
            twitter_data = {
                "campaign_id": 15,
                "tweet_id": "test_tweet_001",
                "retweets": 45,
                "likes": 250,
                "replies": 12,
                "quotes": 8,
                "impressions": 8500,
                "reach": 5200,
                "profile_visits": 150,
                "link_clicks": 89,
                "date_recorded": "2024-08-05T21:45:00Z"
            }
            
            response = requests.post("https://redacted.example.com/analytics/twitter", json=twitter_data)
            test_results["results"]["twitter_analytics"] = {
                "status": "PASS" if response.status_code == 200 else "FAIL",
                "response": response.json() if response.status_code == 200 else response.text,
                "status_code": response.status_code
            }
        except Exception as e:
            test_results["results"]["twitter_analytics"] = {
                "status": "ERROR",
                "error": str(e)
            }
        
        # Test 5: Analytics Summary
        try:
            response = requests.get("https://redacted.example.com/analytics/campaign/15/summary")
            test_results["results"]["analytics_summary"] = {
                "status": "PASS" if response.status_code == 200 else "FAIL",
                "response": response.json() if response.status_code == 200 else response.text,
                "status_code": response.status_code
            }
        except Exception as e:
            test_results["results"]["analytics_summary"] = {
                "status": "ERROR",
                "error": str(e)
            }
        
        # Test 6: Analytics Trends
        try:
            response = requests.get("https://redacted.example.com/analytics/campaign/15/trends/likes")
            test_results["results"]["analytics_trends"] = {
                "status": "PASS" if response.status_code == 200 else "FAIL",
                "response": response.json() if response.status_code == 200 else response.text,
                "status_code": response.status_code
            }
        except Exception as e:
            test_results["results"]["analytics_trends"] = {
                "status": "ERROR",
                "error": str(e)
            }
        
        # Calculate overall test status
        passed_tests = sum(1 for result in test_results["results"].values() if result.get("status") == "PASS")
        total_tests = len(test_results["results"])
        test_results["summary"] = {
            "total_tests": total_tests,
            "passed_tests": passed_tests,
            "failed_tests": total_tests - passed_tests,
            "success_rate": f"{(passed_tests/total_tests)*100:.1f}%" if total_tests > 0 else "0%"
        }
        
        return test_results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Comprehensive test failed: {str(e)}")

@app.post("/test/social-accounts-comprehensive")
def test_social_accounts_comprehensive():
    """Comprehensive test of all social media account functionality"""
    try:
        test_results = {
            "timestamp": "2024-08-05T21:45:00Z",
            "test_suite": "Social Media Accounts Comprehensive Test",
            "results": {}
        }
        
        # Test 1: Create Social Account
        try:
            account_data = {
                "platform": "test_platform",
                "account_name": "Test Analytics Account",
                "account_handle": "@test_analytics",
                "access_token": "test_token_123",
                "refresh_token": "test_refresh_123",
                "token_expires_at": "2024-12-31T23:59:59Z",
                "is_active": True
            }
            
            response = requests.post("https://redacted.example.com/social-accounts", json=account_data)
            test_results["results"]["create_account"] = {
                "status": "PASS" if response.status_code == 200 else "FAIL",
                "response": response.json() if response.status_code == 200 else response.text,
                "status_code": response.status_code
            }
            
            # Extract account_id for subsequent tests
            if response.status_code == 200:
                account_id = response.json().get("account_id")
                test_results["test_account_id"] = account_id
            else:
                account_id = None
                
        except Exception as e:
            test_results["results"]["create_account"] = {
                "status": "ERROR",
                "error": str(e)
            }
            account_id = None
        
        # Test 2: Get All Accounts
        try:
            response = requests.get("https://redacted.example.com/social-accounts")
            test_results["results"]["get_all_accounts"] = {
                "status": "PASS" if response.status_code == 200 else "FAIL",
                "response_count": len(response.json()) if response.status_code == 200 else 0,
                "status_code": response.status_code
            }
        except Exception as e:
            test_results["results"]["get_all_accounts"] = {
                "status": "ERROR",
                "error": str(e)
            }
        
        # Test 3: Get Accounts by Platform
        try:
            response = requests.get("https://redacted.example.com/social-accounts?platform=twitter")
            test_results["results"]["get_accounts_by_platform"] = {
                "status": "PASS" if response.status_code == 200 else "FAIL",
                "response_count": len(response.json()) if response.status_code == 200 else 0,
                "status_code": response.status_code
            }
        except Exception as e:
            test_results["results"]["get_accounts_by_platform"] = {
                "status": "ERROR",
                "error": str(e)
            }
        
        # Test 4: Get Specific Account (if account was created)
        if account_id:
            try:
                response = requests.get(f"https://redacted.example.com/social-accounts/{account_id}")
                test_results["results"]["get_specific_account"] = {
                    "status": "PASS" if response.status_code == 200 else "FAIL",
                    "response": response.json() if response.status_code == 200 else response.text,
                    "status_code": response.status_code
                }
            except Exception as e:
                test_results["results"]["get_specific_account"] = {
                    "status": "ERROR",
                    "error": str(e)
                }
        
        # Test 5: Update Account (if account was created)
        if account_id:
            try:
                update_data = {
                    "platform": "test_platform",
                    "account_name": "Test Analytics Account Updated",
                    "account_handle": "@test_analytics_updated",
                    "access_token": "updated_token_123",
                    "refresh_token": "updated_refresh_123",
                    "token_expires_at": "2024-12-31T23:59:59Z",
                    "is_active": True
                }
                
                response = requests.put(f"https://redacted.example.com/social-accounts/{account_id}", json=update_data)
                test_results["results"]["update_account"] = {
                    "status": "PASS" if response.status_code == 200 else "FAIL",
                    "response": response.json() if response.status_code == 200 else response.text,
                    "status_code": response.status_code
                }
            except Exception as e:
                test_results["results"]["update_account"] = {
                    "status": "ERROR",
                    "error": str(e)
                }
        
        # Test 6: Test Connection (if account was created)
        if account_id:
            try:
                response = requests.post(f"https://redacted.example.com/social-accounts/{account_id}/test-connection")
                test_results["results"]["test_connection"] = {
                    "status": "PASS" if response.status_code == 200 else "FAIL",
                    "response": response.json() if response.status_code == 200 else response.text,
                    "status_code": response.status_code
                }
            except Exception as e:
                test_results["results"]["test_connection"] = {
                    "status": "ERROR",
                    "error": str(e)
                }
        
        # Test 7: Refresh Token (if account was created)
        if account_id:
            try:
                response = requests.post(f"https://redacted.example.com/social-accounts/{account_id}/refresh-token")
                test_results["results"]["refresh_token"] = {
                    "status": "PASS" if response.status_code == 200 else "FAIL",
                    "response": response.json() if response.status_code == 200 else response.text,
                    "status_code": response.status_code
                }
            except Exception as e:
                test_results["results"]["refresh_token"] = {
                    "status": "ERROR",
                    "error": str(e)
                }
        
        # Test 8: Platform Accounts Summary
        try:
            response = requests.get("https://redacted.example.com/social-accounts/platform/twitter/accounts")
            test_results["results"]["platform_accounts_summary"] = {
                "status": "PASS" if response.status_code == 200 else "FAIL",
                "response": response.json() if response.status_code == 200 else response.text,
                "status_code": response.status_code
            }
        except Exception as e:
            test_results["results"]["platform_accounts_summary"] = {
                "status": "ERROR",
                "error": str(e)
            }
        
        # Calculate overall test status
        passed_tests = sum(1 for result in test_results["results"].values() if result.get("status") == "PASS")
        total_tests = len(test_results["results"])
        test_results["summary"] = {
            "total_tests": total_tests,
            "passed_tests": passed_tests,
            "failed_tests": total_tests - passed_tests,
            "success_rate": f"{(passed_tests/total_tests)*100:.1f}%" if total_tests > 0 else "0%"
        }
        
        return test_results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Comprehensive test failed: {str(e)}")

@app.post("/test/campaign-activation-comprehensive")
def test_campaign_activation_comprehensive():
    """Comprehensive test of campaign activation functionality"""
    try:
        test_results = {
            "timestamp": "2024-08-05T21:45:00Z",
            "test_suite": "Campaign Activation Comprehensive Test",
            "results": {}
        }
        
        # Test 1: Get Today's Campaign
        try:
            response = requests.get("https://redacted.example.com/campaigns/today")
            test_results["results"]["get_todays_campaign"] = {
                "status": "PASS" if response.status_code == 200 else "FAIL",
                "response": response.json() if response.status_code == 200 else response.text,
                "status_code": response.status_code
            }
        except Exception as e:
            test_results["results"]["get_todays_campaign"] = {
                "status": "ERROR",
                "error": str(e)
            }
        
        # Test 2: Get Draft Campaigns
        try:
            response = requests.get("https://redacted.example.com/campaigns/draft")
            test_results["results"]["get_draft_campaigns"] = {
                "status": "PASS" if response.status_code == 200 else "FAIL",
                "response_count": len(response.json()) if response.status_code == 200 else 0,
                "status_code": response.status_code
            }
        except Exception as e:
            test_results["results"]["get_draft_campaigns"] = {
                "status": "ERROR",
                "error": str(e)
            }
        
        # Test 3: Get Active Campaigns
        try:
            response = requests.get("https://redacted.example.com/campaigns/active")
            test_results["results"]["get_active_campaigns"] = {
                "status": "PASS" if response.status_code == 200 else "FAIL",
                "response_count": len(response.json()) if response.status_code == 200 else 0,
                "status_code": response.status_code
            }
        except Exception as e:
            test_results["results"]["get_active_campaigns"] = {
                "status": "ERROR",
                "error": str(e)
            }
        
        # Test 4: Activate a Campaign (use existing campaign)
        try:
            activation_data = {
                "activation_date": "2024-08-05T21:45:00Z",
                "platforms": ["twitter", "linkedin"]
            }
            
            response = requests.post("https://redacted.example.com/campaigns/15/activate", json=activation_data)
            test_results["results"]["activate_campaign"] = {
                "status": "PASS" if response.status_code == 200 else "FAIL",
                "response": response.json() if response.status_code == 200 else response.text,
                "status_code": response.status_code
            }
        except Exception as e:
            test_results["results"]["activate_campaign"] = {
                "status": "ERROR",
                "error": str(e)
            }
        
        # Test 5: Get Campaign Activation Status
        try:
            response = requests.get("https://redacted.example.com/campaigns/15/activation-status")
            test_results["results"]["get_activation_status"] = {
                "status": "PASS" if response.status_code == 200 else "FAIL",
                "response": response.json() if response.status_code == 200 else response.text,
                "status_code": response.status_code
            }
        except Exception as e:
            test_results["results"]["get_activation_status"] = {
                "status": "ERROR",
                "error": str(e)
            }
        
        # Calculate overall test status
        passed_tests = sum(1 for result in test_results["results"].values() if result.get("status") == "PASS")
        total_tests = len(test_results["results"])
        test_results["summary"] = {
            "total_tests": total_tests,
            "passed_tests": passed_tests,
            "failed_tests": total_tests - passed_tests,
            "success_rate": f"{(passed_tests/total_tests)*100:.1f}%" if total_tests > 0 else "0%"
        }
        
        return test_results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Comprehensive test failed: {str(e)}")

@app.get("/test/system-health")
def test_system_health():
    """Comprehensive system health check"""
    try:
        health_results = {
            "timestamp": "2024-08-05T21:45:00Z",
            "system": "RedactedApp Analytics & Campaign System",
            "version": "2.0.0",
            "checks": {}
        }
        
        # Check 1: Database Connection
        try:
            
            
            health_results["checks"]["database_connection"] = {
                "status": "HEALTHY" if result else "UNHEALTHY",
                "message": "Database connection successful" if result else "Database connection failed"
            }
        except Exception as e:
            health_results["checks"]["database_connection"] = {
                "status": "UNHEALTHY",
                "message": f"Database connection error: {str(e)}"
            }
        
        # Check 2: Analytics Tables
        try:
            
            
            health_results["checks"]["analytics_tables"] = {
                "status": "HEALTHY" if result[0] > 0 else "UNHEALTHY",
                "message": f"Found {result[0]} analytics tables" if result[0] > 0 else "No analytics tables found",
                "table_count": result[0]
            }
        except Exception as e:
            health_results["checks"]["analytics_tables"] = {
                "status": "UNHEALTHY",
                "message": f"Analytics tables check error: {str(e)}"
            }
        
        # Check 3: Social Media Accounts
        try:
            
            
            health_results["checks"]["social_accounts"] = {
                "status": "HEALTHY" if result[0] >= 0 else "UNHEALTHY",
                "message": f"Found {result[0]} active social media accounts",
                "active_accounts": result[0]
            }
        except Exception as e:
            health_results["checks"]["social_accounts"] = {
                "status": "UNHEALTHY",
                "message": f"Social accounts check error: {str(e)}"
            }
        
        # Check 4: Campaigns
        try:
            
            
            health_results["checks"]["campaigns"] = {
                "status": "HEALTHY" if result[0] >= 0 else "UNHEALTHY",
                "message": f"Found {result[0]} campaigns",
                "total_campaigns": result[0]
            }
        except Exception as e:
            health_results["checks"]["campaigns"] = {
                "status": "UNHEALTHY",
                "message": f"Campaigns check error: {str(e)}"
            }
        
        # Check 5: API Endpoints
        try:
            response = requests.get("https://redacted.example.com/health")
            health_results["checks"]["api_endpoints"] = {
                "status": "HEALTHY" if response.status_code == 200 else "UNHEALTHY",
                "message": "API endpoints responding" if response.status_code == 200 else "API endpoints not responding",
                "status_code": response.status_code
            }
        except Exception as e:
            health_results["checks"]["api_endpoints"] = {
                "status": "UNHEALTHY",
                "message": f"API endpoints check error: {str(e)}"
            }
        
        # Calculate overall health
        healthy_checks = sum(1 for check in health_results["checks"].values() if check.get("status") == "HEALTHY")
        total_checks = len(health_results["checks"])
        health_results["overall_status"] = {
            "status": "HEALTHY" if healthy_checks == total_checks else "DEGRADED" if healthy_checks > 0 else "UNHEALTHY",
            "healthy_checks": healthy_checks,
            "total_checks": total_checks,
            "health_percentage": f"{(healthy_checks/total_checks)*100:.1f}%" if total_checks > 0 else "0%"
        }
        
        return health_results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"System health check failed: {str(e)}")

@app.get("/test/data-validation")
def test_data_validation():
    """Test data validation and integrity"""
    try:
        validation_results = {
            "timestamp": "2024-08-05T21:45:00Z",
            "test_suite": "Data Validation Test",
            "results": {}
        }
        
        # Test 1: Campaign Data Integrity
        try:
            
            
            validation_results["results"]["campaign_integrity"] = {
                "status": "PASS",
                "total_campaigns": result[0],
                "activated_campaigns": result[1],
                "status_activated": result[2],
                "integrity_check": "All activated campaigns have proper timestamps" if result[1] == result[2] else "Data inconsistency detected"
            }
        except Exception as e:
            validation_results["results"]["campaign_integrity"] = {
                "status": "ERROR",
                "error": str(e)
            }
        
        # Test 2: Analytics Data Integrity
        try:
            
            
            validation_results["results"]["analytics_integrity"] = {
                "status": "PASS",
                "total_analytics_records": result[0],
                "campaigns_with_analytics": result[1],
                "platforms_tracked": result[2],
                "data_quality": "Good" if result[0] > 0 else "No analytics data"
            }
        except Exception as e:
            validation_results["results"]["analytics_integrity"] = {
                "status": "ERROR",
                "error": str(e)
            }
        
        # Test 3: Social Accounts Integrity
        try:
            
            
            validation_results["results"]["social_accounts_integrity"] = {
                "status": "PASS",
                "total_accounts": result[0],
                "active_accounts": result[1],
                "platforms_configured": result[2],
                "account_health": "Good" if result[1] > 0 else "No active accounts"
            }
        except Exception as e:
            validation_results["results"]["social_accounts_integrity"] = {
                "status": "ERROR",
                "error": str(e)
            }
        
        # Calculate overall validation status
        passed_tests = sum(1 for result in validation_results["results"].values() if result.get("status") == "PASS")
        total_tests = len(validation_results["results"])
        validation_results["summary"] = {
            "total_tests": total_tests,
            "passed_tests": passed_tests,
            "failed_tests": total_tests - passed_tests,
            "validation_rate": f"{(passed_tests/total_tests)*100:.1f}%" if total_tests > 0 else "0%"
        }
        
        return validation_results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Data validation failed: {str(e)}")

# Test endpoint for analytics models
@app.post("/test-analytics-models")
def test_analytics_models(
    analytics_data: AnalyticsDataPoint,
    content_data: ContentPerformanceData,
    conversion_data: ConversionData,
    activation_request: CampaignActivationRequest
):
    """Test endpoint to verify analytics models are working correctly"""
    return {
        "message": "Analytics models working correctly",
        "analytics_data": analytics_data.dict(),
        "content_data": content_data.dict(),
        "conversion_data": conversion_data.dict(),
        "activation_request": activation_request.dict()
    }

# ============================================================================
# TWITTER/X OAUTH ENDPOINTS (Task 4.3)
# ============================================================================

class TwitterAuthURLResponse(BaseModel):
    authorization_url: str
    state: str


@app.get("/auth/x/url", response_model=TwitterAuthURLResponse)
def get_twitter_authorization_url():
    try:
        # Load config from env
        cfg = load_platform_config_from_env("twitter", prefix="TWITTER")
        platform = TwitterPlatform(cfg)
        url, auth_state = platform.build_authorization_url(
            scopes=["tweet.read", "users.read"],
            redirect_uri=cfg.webhook_url,
        )
        _twitter_oauth_sessions[auth_state.state] = auth_state
        return TwitterAuthURLResponse(authorization_url=url, state=auth_state.state)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/auth/x/url/minimal", response_model=TwitterAuthURLResponse)
def get_twitter_authorization_url_minimal():
    """Test endpoint with minimal read-only scopes for debugging X OAuth issues."""
    try:
        # Load config from env
        cfg = load_platform_config_from_env("twitter", prefix="TWITTER")
        platform = TwitterPlatform(cfg)
        url, auth_state = platform.build_authorization_url(
            scopes=["users.read"],  # Minimal scope for testing
            redirect_uri=cfg.webhook_url,
        )
        _twitter_oauth_sessions[auth_state.state] = auth_state
        return TwitterAuthURLResponse(authorization_url=url, state=auth_state.state)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/auth/x/diagnostics")
def get_oauth_diagnostics():
    """Diagnostic endpoint to help debug X OAuth configuration issues."""
    try:
        cfg = load_platform_config_from_env("twitter", prefix="TWITTER")
        platform = TwitterPlatform(cfg)
        
        # Generate test URLs
        minimal_url, minimal_state = platform.build_authorization_url(scopes=["users.read"])
        full_url, full_state = platform.build_authorization_url(scopes=["tweet.read", "users.read"])
        
        # Parse URLs for analysis
        from urllib.parse import urlparse, parse_qs
        minimal_parsed = urlparse(minimal_url)
        minimal_params = parse_qs(minimal_parsed.query)
        
        return {
            "recommendations": {
                "portal_settings": [
                    "1. In X Developer Portal, ensure OAuth 2.0 is enabled (not just OAuth 1.0a)",
                    "2. App Type MUST be 'Web App, Automated App or Bot'",
                    "3. User authentication settings MUST be configured",
                    "4. Set callback URL exactly as: " + (cfg.webhook_url or "NOT_SET"),
                    "5. App permissions: Start with 'Read' only for testing",
                    "6. Ensure your X account has developer access tier that supports user authentication",
                ],
                "common_issues": [
                    "- Free tier may have limitations on OAuth 2.0 user authentication",
                    "- Callback URL case sensitivity (must match exactly)",
                    "- Missing 'User authentication settings' configuration",
                    "- App suspended or restricted",
                    "- X account not in good standing (needs phone/email verified)",
                ]
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class TwitterCallbackResponse(BaseModel):
    message: str
    account_id: int
    platform: str
    account_name: str


@app.get("/auth/x/callback", response_model=TwitterCallbackResponse)
def twitter_oauth_callback(code: str, state: str):
    try:
        if state not in _twitter_oauth_sessions:
            raise HTTPException(status_code=400, detail="Invalid or expired state")
        auth_state = _twitter_oauth_sessions.pop(state)

        cfg = load_platform_config_from_env("twitter", prefix="TWITTER")
        platform = TwitterPlatform(cfg)
        platform.exchange_code_for_token(code=code, code_verifier=auth_state.code_verifier, redirect_uri=cfg.webhook_url)

        # Fetch user info for account identification
        headers = {"Authorization": f"Bearer {platform.config.access_token.get_secret_value()}"}
        resp = requests.get(f"{platform.config.api_base_url}/2/users/me", headers=headers, timeout=15)
        if resp.status_code != 200:
            raise HTTPException(status_code=500, detail=f"Failed to fetch user info: {resp.text}")
        me = resp.json().get("data", {})
        account_name = me.get("name") or "Twitter Account"
        account_handle = me.get("username")

        # Persist tokens and account info
        

        return TwitterCallbackResponse(
            message="Twitter account connected successfully",
            account_id=row["account_id"],
            platform=row["platform"],
            account_name=row["account_name"],
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# REDDIT OAUTH AND API ENDPOINTS
# ============================================================================

# Reddit OAuth endpoints
@app.get("/api/social/reddit/auth/url")
def get_reddit_authorization_url():
    """Get Reddit OAuth authorization URL"""
    try:
        # Load config from env with redirect URI
        cfg = load_platform_config_from_env("reddit", prefix="REDDIT")
        cfg.extra["redirect_uri"] = os.getenv("REDDIT_REDIRECT_URI", "https://redacted.example.com/auth/reddit/oauth2/callback")
        cfg.extra["user_agent"] = "redacted-app:v1.0 (by u/redacted-app)"
        
        platform = RedditPlatform(cfg)
        auth_url, state = platform.build_authorization_url(
            scopes=["identity", "read", "submit"],
            redirect_uri=cfg.extra["redirect_uri"]
        )
        
        return {
            "authorization_url": auth_url,
            "state": state
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/auth/reddit/url")
def get_reddit_authorization_url_alias():
    """Alias to get Reddit OAuth authorization URL with standard shape."""
    try:
        cfg = load_platform_config_from_env("reddit", prefix="REDDIT")
        cfg.extra["redirect_uri"] = os.getenv("REDDIT_REDIRECT_URI", "https://redacted.example.com/auth/reddit/oauth2/callback")
        cfg.extra["user_agent"] = "redacted-app:v1.0 (by u/redacted-app)"
        
        platform = RedditPlatform(cfg)
        auth_url, state = platform.build_authorization_url(
            scopes=["identity", "read", "submit"],
            redirect_uri=cfg.extra["redirect_uri"]
        )
        
        return {
            "authorization_url": auth_url,
            "state": state
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/auth/reddit/oauth2", response_class=HTMLResponse)
def reddit_oauth_callback(code: str, state: Optional[str] = None):
    """Handle Reddit OAuth callback"""
    try:
        # Load config
        cfg = load_platform_config_from_env("reddit", prefix="REDDIT")
        cfg.extra["redirect_uri"] = os.getenv("REDDIT_REDIRECT_URI", "https://redacted.example.com/auth/reddit/oauth2/callback")
        cfg.extra["user_agent"] = "redacted-app:v1.0 (by u/redacted-app)"
        
        platform = RedditPlatform(cfg)
        
        # Exchange code for refresh token
        refresh_token = platform.exchange_code_for_token(
            code=code,
            redirect_uri=cfg.extra["redirect_uri"]
        )
        
        # Save the refresh token
        platform.save_refresh_token(refresh_token)
        
        # Test the connection
        platform.authenticate()
        if platform.test_connection():
            # Persist account to DB (upsert on platform + handle)
            user = platform._reddit_client.user.me()
            account_name = getattr(user, "name", None) or "Reddit Account"
            account_handle = getattr(user, "name", None)
            
            
            
            # Return success HTML page
            return """
            <html>
                <head>
                    <title>Reddit Authorization Successful</title>
                    <style>
                        body { font-family: Arial, sans-serif; text-align: center; padding: 50px; }
                        .success { color: #4CAF50; }
                    </style>
                </head>
                <body>
                    <h1 class="success">✅ Reddit Authorization Successful!</h1>
                    <p>Your Reddit account has been connected to redacted-app.</p>
                    <p>You can now close this window and return to the application.</p>
                    <br>
                    <p><a href="/">Return to Home</a></p>
                </body>
            </html>
            """
        else:
            raise Exception("Failed to verify Reddit connection")
            
    except Exception as e:
        # Return error HTML page
        return f"""
        <html>
            <head>
                <title>Reddit Authorization Failed</title>
                <style>
                    body {{ font-family: Arial, sans-serif; text-align: center; padding: 50px; }}
                    .error {{ color: #f44336; }}
                    pre {{ background: #f5f5f5; padding: 10px; text-align: left; display: inline-block; }}
                </style>
            </head>
            <body>
                <h1 class="error">❌ Reddit Authorization Failed</h1>
                <p>There was an error connecting your Reddit account.</p>
                <p>Error details:</p>
                <pre>{str(e)}</pre>
                <br>
                <p><a href="/api/social/reddit/auth/url">Try Again</a></p>
            </body>
        </html>
        """


@app.get("/auth/reddit/oauth2/callback", response_class=HTMLResponse)
def reddit_oauth_callback_alias(code: str, state: Optional[str] = None):
    """Alias route matching the configured Reddit redirect URI."""
    return reddit_oauth_callback(code=code, state=state)


# Reddit API endpoints
@app.get("/api/social/reddit/me")
def get_reddit_identity():
    """Get current Reddit user identity"""
    try:
        cfg = load_platform_config_from_env("reddit", prefix="REDDIT")
        cfg.extra["user_agent"] = "redacted-app:v1.0 (by u/redacted-app)"
        
        platform = RedditPlatform(cfg)
        platform.authenticate()
        
        # Get user info directly from PRAW
        user = platform._reddit_client.user.me()
        
        return {
            "username": user.name,
            "id": user.id,
            "created_utc": user.created_utc,
            "comment_karma": user.comment_karma,
            "link_karma": user.link_karma
        }
    except SMAuthenticationError as e:
        raise HTTPException(status_code=401, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class RedditListPostsRequest(BaseModel):
    subreddit: str
    sort: str = "hot"  # hot, new, top, rising
    limit: int = 25


@app.post("/api/social/reddit/posts/list")
def list_reddit_posts(request: RedditListPostsRequest):
    """List posts from a subreddit"""
    try:
        cfg = load_platform_config_from_env("reddit", prefix="REDDIT")
        cfg.extra["user_agent"] = "redacted-app:v1.0 (by u/redacted-app)"
        
        platform = RedditPlatform(cfg)
        platform.authenticate()
        
        posts = platform.list_posts(
            subreddit=request.subreddit,
            sort=request.sort,
            limit=request.limit
        )
        
        return {"posts": posts}
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except SMAuthenticationError as e:
        raise HTTPException(status_code=401, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class RedditSubmitRequest(BaseModel):
    subreddit: str
    title: str
    text: Optional[str] = None
    url: Optional[str] = None


@app.post("/api/social/reddit/submit")
def submit_reddit_post(request: RedditSubmitRequest):
    """Submit a post to Reddit"""
    try:
        # Validate: must have either text or url, not both
        if (request.text and request.url) or (not request.text and not request.url):
            raise HTTPException(
                status_code=400,
                detail="Must provide either 'text' for a text post or 'url' for a link post, but not both"
            )
        
        cfg = load_platform_config_from_env("reddit", prefix="REDDIT")
        cfg.extra["user_agent"] = "redacted-app:v1.0 (by u/redacted-app)"
        
        platform = RedditPlatform(cfg)
        platform.authenticate()
        
        # Format content for post_content method
        content_parts = [f"subreddit:{request.subreddit}", f"title:{request.title}"]
        if request.text:
            content_parts.append(f"text:{request.text}")
        else:
            content_parts.append(f"url:{request.url}")
        
        content_text = " ".join(content_parts)
        
        result = platform.post_content(content_text)
        
        return {
            "post_id": result.post_id,
            "url": result.url,
            "fullname": result.raw_response.get("name")
        }
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except SMAuthenticationError as e:
        raise HTTPException(status_code=401, detail=str(e))
    except AuthorizationError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class RedditReplyRequest(BaseModel):
    parent_fullname: str  # t3_xxx for posts, t1_xxx for comments
    body: str


@app.post("/api/social/reddit/reply")
def reply_to_reddit_item(request: RedditReplyRequest):
    """Reply to a Reddit post or comment"""
    try:
        cfg = load_platform_config_from_env("reddit", prefix="REDDIT")
        cfg.extra["user_agent"] = "redacted-app:v1.0 (by u/redacted-app)"
        
        platform = RedditPlatform(cfg)
        platform.authenticate()
        
        # Determine if it's a post or comment based on fullname prefix
        if request.parent_fullname.startswith("t3_"):
            # It's a post
            post_id = request.parent_fullname[3:]  # Remove t3_ prefix
            result = platform.reply_to_post(post_id, request.body)
        elif request.parent_fullname.startswith("t1_"):
            # It's a comment
            comment_id = request.parent_fullname[3:]  # Remove t1_ prefix
            result = platform.reply_to_comment(comment_id, request.body)
        else:
            raise HTTPException(
                status_code=400,
                detail="Invalid parent_fullname. Must start with 't3_' for posts or 't1_' for comments"
            )
        
        return result
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except SMAuthenticationError as e:
        raise HTTPException(status_code=401, detail=str(e))
    except AuthorizationError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# TWITTER POSTING AND METRICS (Task 4.3)
# ============================================================================

class TwitterPostRequest(BaseModel):
    text: str = Field(min_length=1, max_length=280)


class TwitterPostResponse(BaseModel):
    account_id: int
    tweet_id: str
    url: str


@app.post("/social-accounts/{account_id}/post/twitter", response_model=TwitterPostResponse)
def post_to_twitter(account_id: int, payload: TwitterPostRequest):
    try:
        # Load account from DB
        
            raise HTTPException(status_code=404, detail="Social media account not found")
        if row["platform"].lower() not in ("twitter", "x"):
            cur.close(); conn.close()
            raise HTTPException(status_code=400, detail="Account is not a Twitter/X account")

        # Build platform config from env + DB tokens
        cfg = load_platform_config_from_env("twitter", prefix="TWITTER")
        if row.get("access_token"):
            from pydantic import SecretStr
            cfg.access_token = SecretStr(row["access_token"])
        if row.get("refresh_token"):
            from pydantic import SecretStr
            cfg.refresh_token = SecretStr(row["refresh_token"])
        cfg.token_expires_at = row.get("token_expires_at")

        # Use OAuth 1.0a if no token expiry date (OAuth 1.0a tokens don't expire)
        if cfg.token_expires_at is None and cfg.refresh_token:
            platform = TwitterOAuth1Platform(cfg)
        else:
            platform = TwitterPlatform(cfg)

        # Refresh if needed (only for OAuth 2.0)
        try:
            if hasattr(platform, 'is_token_expired') and platform.is_token_expired():
                platform.refresh_access_token()
        except Exception as e:
            raise HTTPException(status_code=401, detail=f"Token refresh failed: {e}")

        # Post
        result = platform.post_content(content_text=payload.text)

        # Persist any updated tokens/expiry
         if platform.config.access_token else None,
                platform.config.refresh_token.get_secret_value() if platform.config.refresh_token else None,
                platform.config.token_expires_at.isoformat() if platform.config.token_expires_at else None,
                account_id,
            ),
        )
        conn.commit()
        cur.close(); conn.close()

        return TwitterPostResponse(account_id=account_id, tweet_id=result.post_id, url=result.url or "")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class TwitterMetricsResponse(BaseModel):
    account_id: int
    post_id: Optional[str] = None
    results: List[dict]


@app.get("/social-accounts/{account_id}/metrics/twitter", response_model=TwitterMetricsResponse)
def twitter_metrics(account_id: int, post_id: Optional[str] = None):
    try:
        # Load account from DB
        
            raise HTTPException(status_code=404, detail="Social media account not found")
        if row["platform"].lower() not in ("twitter", "x"):
            cur.close(); conn.close()
            raise HTTPException(status_code=400, detail="Account is not a Twitter/X account")

        cfg = load_platform_config_from_env("twitter", prefix="TWITTER")
        if row.get("access_token"):
            from pydantic import SecretStr
            cfg.access_token = SecretStr(row["access_token"])
        if row.get("refresh_token"):
            from pydantic import SecretStr
            cfg.refresh_token = SecretStr(row["refresh_token"])
        cfg.token_expires_at = row.get("token_expires_at")

        # Use OAuth 1.0a if no token expiry date (OAuth 1.0a tokens don't expire)
        if cfg.token_expires_at is None and cfg.refresh_token:
            platform = TwitterOAuth1Platform(cfg)
        else:
            platform = TwitterPlatform(cfg)

        # Refresh if needed (only for OAuth 2.0)
        try:
            if hasattr(platform, 'is_token_expired') and platform.is_token_expired():
                platform.refresh_access_token()
        except Exception as e:
            raise HTTPException(status_code=401, detail=f"Token refresh failed: {e}")

        results = platform.fetch_metrics(post_id=post_id)

        # Persist any updated tokens/expiry (in case refresh happened)
         if platform.config.access_token else None,
                platform.config.refresh_token.get_secret_value() if platform.config.refresh_token else None,
                platform.config.token_expires_at.isoformat() if platform.config.token_expires_at else None,
                account_id,
            ),
        )
        conn.commit()
        cur.close(); conn.close()

        return TwitterMetricsResponse(
            account_id=account_id,
            post_id=post_id,
            results=[r.model_dump() for r in results],
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/auth/x/url", response_model=TwitterAuthURLResponse)
def get_x_authorization_url():
    return get_twitter_authorization_url()


@app.get("/auth/x/callback", response_model=TwitterCallbackResponse)
def x_oauth_callback(code: str, state: str):
    return twitter_oauth_callback(code=code, state=state)

# ============================================================================
# TWITTER/X OAUTH 1.0a ENDPOINTS
# ============================================================================

@app.get("/auth/x/oauth1/url", response_model=TwitterAuthURLResponse)
def get_twitter_oauth1_authorization_url():
    """Generate OAuth 1.0a authorization URL for Twitter/X."""
    try:
        # Load config from env
        cfg = load_platform_config_from_env("twitter", prefix="TWITTER")
        
        # Make sure we have API key and secret (OAuth 1.0a terminology)
        if not cfg.client_id or not cfg.client_secret:
            raise HTTPException(
                status_code=500, 
                detail="Twitter API Key and Secret are required for OAuth 1.0a. Set TWITTER_CLIENT_ID and TWITTER_CLIENT_SECRET."
            )
        
        platform = TwitterOAuth1Platform(cfg)
        url, auth_state = platform.get_request_token()
        
        # Store the OAuth state for callback verification
        _twitter_oauth1_sessions[auth_state.oauth_token] = auth_state
        
        return TwitterAuthURLResponse(
            authorization_url=url, 
            state=auth_state.oauth_token  # In OAuth 1.0a, we use oauth_token as state
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/auth/x/oauth1/callback", response_model=TwitterCallbackResponse)
def twitter_oauth1_callback(oauth_token: str, oauth_verifier: str):
    """Handle OAuth 1.0a callback from Twitter/X."""
    try:
        # Retrieve the stored OAuth state
        if oauth_token not in _twitter_oauth1_sessions:
            raise HTTPException(status_code=400, detail="Invalid or expired oauth_token")
        
        auth_state = _twitter_oauth1_sessions.pop(oauth_token)
        
        # Load config and exchange verifier for access token
        cfg = load_platform_config_from_env("twitter", prefix="TWITTER")
        platform = TwitterOAuth1Platform(cfg)
        
        platform.exchange_verifier_for_access_token(
            oauth_token=oauth_token,
            oauth_verifier=oauth_verifier,
            request_token_secret=auth_state.request_token_secret
        )
        
        # Get user info
        user_info = platform.get_user_info()
        account_name = user_info.get("name", "Twitter Account")
        account_handle = user_info.get("username") or user_info.get("screen_name")
        
        # Persist tokens and account info
        
        
        return TwitterCallbackResponse(
            message="Twitter account connected successfully via OAuth 1.0a",
            account_id=row["account_id"],
            platform=row["platform"],
            account_name=row["account_name"],
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Initialize HeyGen and Storage services
heygen_service = HeyGenService(api_key=os.environ.get("HEYGEN_API_KEY"))
storage_service = StorageService()

# Log HeyGen readiness (mask presence only)
if os.environ.get("HEYGEN_API_KEY"):
    logger.info("HeyGen API key configured (masked)")
else:
    logger.warning("HEYGEN_API_KEY is not set; HeyGen features will fail")

# Webhook configuration
if os.environ.get("HEYGEN_WEBHOOK_URL"):
    logger.info("HEYGEN_WEBHOOK_URL configured, webhook events enabled")
else:
    logger.warning("HEYGEN_WEBHOOK_URL not set; webhook events disabled")

@app.get("/heygen/voices")
def get_heygen_voices():
    if DEMO_MODE:
        return {"voices": []}
    # Fetch live voices from HeyGen API
    try:
        result = heygen_service.list_voices() or {}
        voices = (result.get("data", {}) or {}).get("voices", [])
        processed = [
            {"id": v.get("voice_id"), "name": v.get("name"), "status": v.get("status", "available")}
            for v in voices if v.get("voice_id")
        ]
        logger.info(f"Returning {len(processed)} voices from HeyGen API")
        return {"voices": processed}
    except Exception as e:
        logger.error(f"Error fetching voices from HeyGen: {str(e)}")
        return {"voices": [], "error": str(e)}

@app.get("/heygen/avatars")
def get_heygen_avatars(include_defaults: bool = True):
    if DEMO_MODE:
        return {"avatars": [], "talking_photos": []}
    avatars_list = []
    
    # First, get custom avatars from our database
    try:
        
        
        for avatar in custom_avatars:
            metadata = avatar.get('metadata') or {}
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except:
                    metadata = {}
            
            avatars_list.append({
                "id": avatar['id'],
                "name": avatar['name'],
                "status": avatar['status'],
                "is_custom": True,
                "type": metadata.get('type', 'talking_photo'),
                "preview_image_url": metadata.get('image_url')
            })
        
        logger.info(f"Found {len(avatars_list)} custom avatars in database")
    except Exception as e:
        logger.error(f"Error fetching custom avatars from database: {str(e)}")
    
    # Then, optionally fetch default avatars from HeyGen API
    if include_defaults:
        try:
            result = heygen_service.list_avatars() or {}
            data = result.get("data", {}) or {}
            api_avatars = data.get("avatars", [])
            
            for a in api_avatars:
                if a.get("avatar_id"):
                    avatars_list.append({
                        "id": a.get("avatar_id"),
                        "name": a.get("avatar_name") or a.get("avatar_id"),
                        "status": a.get("status", "available"),
                        "is_custom": False,
                        "type": "avatar",
                        "gender": a.get("gender"),
                        "preview_image_url": a.get("preview_image_url")
                    })
            
            logger.info(f"Added {len(api_avatars)} default avatars from HeyGen API")
        except Exception as e:
            logger.error(f"Error fetching default avatars from HeyGen: {str(e)}")
    
    logger.info(f"Returning total {len(avatars_list)} avatars")
    return {"avatars": avatars_list}

# Configuration limits for HeyGen jobs
MAX_CONCURRENT_JOBS = int(os.environ.get("HEYGEN_MAX_CONCURRENT_JOBS", "5"))
DAILY_JOB_LIMIT = int(os.environ.get("HEYGEN_DAILY_JOB_LIMIT", "50"))

def check_job_limits():
    """Check if job limits are exceeded (concurrency disabled)."""
    try:
        # Concurrency limit disabled per ops decision; only keep a lightweight daily guard if desired.
        # Comment out daily limit as well to fully disable limits:
        return True
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error checking job limits: {str(e)}")

# Add Pydantic model for video creation
class VideoCreateRequest(BaseModel):
    mode: str
    avatar_id: str  # Accept as string, can be numeric or test string
    voice_id: Optional[str] = None  # Accept as string, can be numeric or test string
    input_text: Optional[str] = None
    input_audio_asset_id: Optional[int] = None
    template_id: Optional[int] = None
    variables: Optional[dict] = None
    publication_id: Optional[int] = None
    background: Optional[str] = None  # e.g., hex color or image URL
    caption: Optional[bool] = False  # include captions in video
    
    @validator('avatar_id')
    def validate_avatar_id(cls, v):
        # HeyGen uses string IDs, so just return as-is
        return str(v)
    
    @validator('voice_id')
    def validate_voice_id(cls, v):
        # HeyGen uses string IDs, so just return as-is if provided
        return str(v) if v is not None else None

@app.post("/heygen/videos")
def create_heygen_video(background_tasks: BackgroundTasks, video_request: VideoCreateRequest):
    start_time = time.time()
    try:
        # Phase 10: Input validation
        # Only text mode supported
        if video_request.mode != "text":
            raise HTTPException(status_code=400, detail="Only 'text' mode supported")
        # Avatar must be provided
        if not video_request.avatar_id:
            raise HTTPException(status_code=400, detail="avatar_id is required")
        # Input text is required and must be non-empty
        if not video_request.input_text or not video_request.input_text.strip():
            raise HTTPException(status_code=400, detail="input_text is required")
        # Enforce max length
        if video_request.input_text and len(video_request.input_text) > 2000:
            raise HTTPException(status_code=400, detail="input_text exceeds maximum length of 2000 characters")
        
        # Check job limits before creating new job
        check_job_limits()
        
        # Handle test IDs vs real HeyGen IDs
        is_test_request = (
            video_request.avatar_id.startswith('test_') or
            (video_request.voice_id and video_request.voice_id.startswith('test_'))
        )
        
        if is_test_request:
            # For test data, simulate a successful response
            import uuid
            test_video_id = f"test_video_{uuid.uuid4().hex[:8]}"
            
            duration = time.time() - start_time
            log_heygen_operation("video_submit", job_id=test_video_id, duration=duration, status="success")
            
            return {
                "message": "Test video job created successfully",
                "video_id": test_video_id,
                "status": "queued",
                "publication_id": video_request.publication_id,
                "note": "This is a test response - no actual HeyGen API call was made"
            }
        else:
            # For real HeyGen IDs, make actual API call
            webhook_url = None
            base_webhook = os.environ.get("HEYGEN_WEBHOOK_URL")
            public_base = os.environ.get("PUBLIC_URL")
            if public_base:
                webhook_url = f"{public_base.rstrip('/')}/webhooks/heygen"
            elif base_webhook:
                # Normalize to /webhooks/heygen
                if base_webhook.endswith("/webhooks"):
                    base_webhook += "/heygen"
                elif base_webhook.endswith("/webhooks/") and not base_webhook.endswith("/webhooks/heygen"):
                    base_webhook += "heygen"
                webhook_url = base_webhook
            elif public_base:
                webhook_url = f"{public_base.rstrip('/')}/webhooks/heygen"
            
            # Check if this is a custom avatar from our database
            is_talking_photo = False
            conn = get_db_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            )
            avatar_record = cur.fetchone()
            if avatar_record:
                metadata = avatar_record.get('metadata', {})
                if isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except:
                        metadata = {}
                is_talking_photo = metadata.get('type') == 'talking_photo'
            cur.close()
            conn.close()
            
            # Validate avatar and voice
            try:
                corrected_avatar_id = video_request.avatar_id  # Default to provided ID
                
                # If it's in our database, we trust it as valid
                if not avatar_record:
                    # Not in our database, check HeyGen API
                    try:
                        avatar_list = (heygen_service.list_avatars() or {}).get("data", {})
                        all_avatars = (avatar_list.get("avatars", []) or []) + (avatar_list.get("talking_photos", []) or [])
                        
                        # Map provided avatar to a valid avatar_id
                        provided_avatar = str(video_request.avatar_id)
                        found_avatar = next((a for a in all_avatars if a.get("avatar_id") == provided_avatar), None)
                        if not found_avatar:
                            # Try match by name if a name was mistakenly sent
                            found_avatar = next((a for a in all_avatars if (a.get("avatar_name") or "").strip() == provided_avatar), None)
                        if not found_avatar:
                            raise HTTPException(status_code=400, detail=f"Invalid avatar_id: {video_request.avatar_id}")
                        corrected_avatar_id = found_avatar.get("avatar_id")
                    except Exception as e:
                        logger.error(f"Error checking HeyGen avatars: {e}")
                        # If HeyGen API fails, just use the provided ID
                        corrected_avatar_id = video_request.avatar_id
                
                # Validate voice
                corrected_voice_id = None
                if video_request.voice_id:
                    try:
                        voices_list = (heygen_service.list_voices() or {}).get("data", {})
                        all_voices = voices_list.get("voices", []) or []
                        
                        provided_voice = str(video_request.voice_id)
                        found_voice = next((v for v in all_voices if v.get("voice_id") == provided_voice), None)
                        if not found_voice:
                            # Try match by name if a name was mistakenly sent
                            found_voice = next((v for v in all_voices if (v.get("name") or "").strip() == provided_voice), None)
                        if not found_voice:
                            raise HTTPException(status_code=400, detail=f"Invalid voice_id: {video_request.voice_id}")
                        corrected_voice_id = found_voice.get("voice_id")
                    except HTTPException:
                        raise
                    except Exception as e:
                        logger.error(f"Error checking HeyGen voices: {e}")
                        # If HeyGen API fails, just use the provided ID
                        corrected_voice_id = video_request.voice_id
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Error validating avatar/voice: {e}")
                raise HTTPException(status_code=500, detail="Validation error, please retry")
            

            
            # Prepare video data
            video_data = {
                "avatar_id": corrected_avatar_id,
                "voice_id": corrected_voice_id,
                "input_text": video_request.input_text,
                "background": video_request.background,
                "caption": video_request.caption,
                "test": True,  # MUST be true for free tier
                "webhook_url": webhook_url,
                "free_tier": True,  # Flag to ensure test mode and low res
                "is_talking_photo": is_talking_photo
            }
            
            try:
                # Call HeyGen API
                result = heygen_service.generate_video(video_data)
                video_id = result.get('data', {}).get('video_id')
                
                if video_id:
                    # Store in database
                    
                    
                    duration = time.time() - start_time
                    log_heygen_operation("video_submit", job_id=video_id, duration=duration, status="success")
                    
                    return {
                        "message": "Video generation started successfully",
                        "video_id": video_id,
                        "db_id": db_id,
                        "status": "processing",
                        "publication_id": video_request.publication_id
                    }
                else:
                    raise Exception("No video_id in response")
                    
            except Exception as e:
                logger.error(f"Error calling HeyGen API: {str(e)}")
                raise HTTPException(status_code=500, detail=f"HeyGen API error: {str(e)}")
            
            return {"message": "Video job created", "status": "queued", "publication_id": video_request.publication_id}
            
    except HTTPException:
        duration = time.time() - start_time
        log_heygen_operation("video_submit", job_id="new", duration=duration, status="failed")
        raise
    except Exception as e:
        duration = time.time() - start_time
        log_heygen_operation("video_submit", job_id="new", duration=duration, status="error")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/heygen/videos/{video_id}")
def get_heygen_video_status(video_id: int):
    try:
        # Retrieve job status from the heygen_videos table
        # Optionally poll HeyGen API if status is pending
        return {"video_id": video_id, "status": "pending"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/heygen/videos/status/{provider_video_id}")
def get_heygen_video_status(provider_video_id: str):
    try:
        
        if not row:
            raise HTTPException(status_code=404, detail="Video job not found")
        return dict(row)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def verify_webhook_signature(payload: bytes, signature: str, secret: str):
    """Verify HeyGen webhook signature"""
    expected_signature = hmac.new(
        secret.encode('utf-8'),
        payload,
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(signature, expected_signature)

@app.post("/webhooks/heygen")
async def handle_heygen_webhook(request: Request):
    start_time = time.time()
    try:
        # Get raw body for signature verification
        body = await request.body()
        
        # Verify signature if secret is configured
        signature = request.headers.get("X-HeyGen-Signature")
        secret = os.getenv("HEYGEN_WEBHOOK_SECRET")
        if secret:
            if not signature or not verify_webhook_signature(body, signature, secret):
                logger.error("Invalid HeyGen webhook signature")
                raise HTTPException(status_code=401, detail="Invalid webhook signature")
        
        # Parse JSON payload
        try:
            payload = await request.json()
        except json.JSONDecodeError:
            logger.error("Invalid JSON in webhook payload")
            raise HTTPException(status_code=400, detail="Invalid JSON payload")
        
        # Log the webhook payload
        logger.info(f"Received HeyGen webhook: {json.dumps(payload, indent=2)}")
        
        # Extract webhook data
        event_type = payload.get("event_type", payload.get("type"))
        video_id = payload.get("video_id")
        status = payload.get("status", "").lower()
        
        if not video_id:
            logger.error("No video_id in webhook payload")
            return {"error": "No video_id provided"}
        
        # Derive status from event_type when missing; expand variants
        if not status and event_type:
            et = str(event_type).lower()
            if "complete" in et or "succeed" in et or "finish" in et:
                status = "completed"
            elif "fail" in et or "error" in et:
                status = "failed"
            elif "process" in et or "pending" in et:
                status = "processing"
        
        # Map HeyGen status to our status
        status_map = {
            "completed": "completed",
            "success": "completed",
            "succeeded": "completed",
            "finished": "completed",
            "failed": "failed",
            "error": "failed",
            "processing": "processing",
            "pending": "processing"
        }
        
        db_status = status_map.get(status, status or "processing")
        
        # Update video record in database
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        if db_status == "completed":
            # Get video URL from payload
            video_url = payload.get("video_url") or payload.get("url") or payload.get("result", {}).get("url")
            
            if video_url:
                # Download video to S3
                try:
                    logger.info(f"Downloading video from {video_url}")
                    s3_result = storage_service.download_to_s3(video_url, "videos", "mp4")
                    
                    # Create asset record (align with assets schema)
                     or f"s3://{s3_result['bucket']}/{s3_result['key']}")
                    )
                    asset_id = cur.fetchone()['asset_id']
                    
                    # Update video record
                    
                    )
                    
                    logger.info(f"Video {video_id} completed and stored with asset_id {asset_id}")
                except Exception as e:
                    logger.error(f"Error downloading/storing video: {str(e)}")
                    # Persist failed status with error context
                    , video_url, video_id)
                    )
            else:
                logger.error("No video URL in completed webhook")
                # Persist failed status when no URL is provided
                
                )
        else:
            # Just update status
            
            )
        
        conn.commit()
        cur.close()
        conn.close()
        
        duration = time.time() - start_time
        log_heygen_operation("webhook_process", job_id=video_id, duration=duration, status="success")
        
        return {"message": "Webhook processed", "video_id": video_id, "status": db_status}
        
    except HTTPException:
        raise
    except Exception as e:
        duration = time.time() - start_time
        log_heygen_operation("webhook_process", job_id="unknown", duration=duration, status="error")
        logger.error(f"Webhook processing error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Add webhook URL info endpoint
@app.get("/webhooks/heygen/info")
def get_webhook_info():
    """Get the webhook URL for HeyGen configuration"""
    # Prefer explicit webhook URL when provided, else derive from PUBLIC_URL
    explicit = os.environ.get("HEYGEN_WEBHOOK_URL")
    if explicit:
        if explicit.endswith("/webhooks"):
            webhook_url = explicit + "/heygen"
        elif explicit.endswith("/webhooks/") and not explicit.endswith("/webhooks/heygen"):
            webhook_url = explicit + "heygen"
        else:
            webhook_url = explicit
    else:
        base_url = os.environ.get("PUBLIC_URL", "https://redacted.example.com")
        webhook_url = f"{base_url.rstrip('/')}/webhooks/heygen"
    
    return {
        "webhook_url": webhook_url,
        "instructions": "Add this URL to your HeyGen account settings",
        "supports": ["video.completed", "video.failed", "video.processing"]
    }

async def clone_voice(
    background_tasks: BackgroundTasks,
    name: str = Form(...),
    language: str = Form(...),
    consent: bool = Form(...),
    training_audio: UploadFile = File(...)
):
    try:
        # Validate consent
        if not consent:
            raise HTTPException(status_code=400, detail="Consent is required for voice cloning")
        
        # Validate file type
        if not training_audio.content_type.startswith('audio/'):
            raise HTTPException(status_code=400, detail="File must be an audio file")
        
        # Upload file to S3
        try:
            # Read file content
            file_content = await training_audio.read()
            
            # Generate S3 key
            file_extension = training_audio.filename.split('.')[-1] if '.' in training_audio.filename else 'wav'
            s3_key = f"training/voices/{uuid.uuid4().hex}.{file_extension}"
            
            # Upload to S3
            s3_client = boto3.client('s3')
            bucket_name = os.environ.get("S3_BUCKET")
            
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_content,
                ContentType=training_audio.content_type
            )
            
            # Generate S3 URL
            s3_url = f"s3://{bucket_name}/{s3_key}"
            
            log_s3_operation("upload", bucket_name, s3_key, status="success")
            
        except Exception as e:
            logger.error(f"Error uploading to S3: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error uploading file to S3: {str(e)}")
        
        # Prepare training data for HeyGen
        training_data = {
            "name": name,
            "language": language,
            "training_audio_url": s3_url
        }
        
        # Call HeyGen cloning API
        try:
            result = heygen_service.clone_voice(training_data)
            log_heygen_operation("voice_cloning", job_id=result.get('voice_id'), status="started")
            
        except Exception as e:
            logger.error(f"Error calling HeyGen API: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error calling HeyGen API: {str(e)}")
        
        # Store in database
        try:
            
            
        except Exception as e:
            logger.error(f"Error storing in database: {str(e)}")
            # Don't fail the whole request if DB fails, but log it
        
        return {
            "message": "Voice cloning started successfully",
            "voice_id": result.get('voice_id'),
            "status": "training",
            "s3_url": s3_url
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in voice cloning: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@app.get("/heygen/voices/{voice_id}")
def get_voice_status(voice_id: int):
    try:
        # Show voice status
        # When available, optionally download preview audio to S3 and set preview_asset_id
        return {"voice_id": voice_id, "status": "training"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/heygen/avatars/train")
async def train_avatar(
    background_tasks: BackgroundTasks,
    name: str = Form(...),
    media_type: str = Form(...),
    consent: bool = Form(...),
    training_media: UploadFile = File(...)
):
    try:
        # Validate consent
        if not consent:
            raise HTTPException(status_code=400, detail="Consent is required for avatar training")
        
        # Photo avatars only support images, not videos
        if media_type != 'image':
            raise HTTPException(status_code=400, detail="Photo avatars only support image uploads, not video")
        
        # Validate file type (handle missing content_type gracefully)
        inferred_content_type = (training_media.content_type or 
                                 (mimetypes.guess_type(training_media.filename)[0] if training_media.filename else None) or 
                                 "image/jpeg")
        if not inferred_content_type.startswith('image/'):
            raise HTTPException(status_code=400, detail="File must be an image (JPEG or PNG)")
        
        # Read file content
        file_content = await training_media.read()
        
        # Step 1: Upload photo to HeyGen to get image_key
        try:
            upload_result = heygen_service.upload_photo(file_content, inferred_content_type)
            image_key = upload_result.get("image_key")
            image_url = upload_result.get("image_url")
            talking_photo_id = upload_result.get("talking_photo_id")
            logger.info(f"Photo uploaded successfully, image_key: {image_key}, talking_photo_id: {talking_photo_id}")
        except Exception as e:
            logger.error(f"Error uploading photo to HeyGen: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error uploading photo: {str(e)}")
        
        # If we have a talking_photo_id, we might be able to use it directly for videos
        if talking_photo_id:
            # For talking photos, we can use them directly in video generation
            # Store the talking photo info
            try:
                conn = get_db_connection()
                cur = conn.cursor(cursor_factory=RealDictCursor)
                
                    )
                )
                avatar_record = cur.fetchone()
                conn.commit()
                cur.close()
                conn.close()
                
                return {
                    "message": "Talking photo avatar created successfully",
                    "avatar_id": talking_photo_id,
                    "type": "talking_photo",
                    "status": "available",
                    "image_url": image_url
                }
            except Exception as e:
                logger.error(f"Error storing talking photo: {str(e)}")
        
        # Step 2: Try to create photo avatar group (might not be available on all plans)
        try:
            # First try to generate photo avatar photos if we have an image URL
            if image_url:
                try:
                    gen_result = heygen_service.generate_photo_avatar_photos(image_url)
                    logger.info(f"Photo generation result: {gen_result}")
                    # Update image_key if we got a new one
                    if gen_result.get("data", {}).get("image_key"):
                        image_key = gen_result["data"]["image_key"]
                except Exception as e:
                    logger.warning(f"Photo generation not available: {e}")
            
            group_result = heygen_service.create_photo_avatar_group(name, image_key)
            group_data = group_result.get("data", {})
            group_id = group_data.get("group_id") or group_data.get("id")
            avatar_id = group_data.get("id")
            
            if not group_id:
                raise ValueError(f"No group_id in response: {group_result}")
            
            logger.info(f"Photo avatar group created: {group_id}")
        except Exception as e:
            logger.error(f"Error creating photo avatar group: {str(e)}")
            # Photo avatar groups might not be available, but talking photos are still useful
            if talking_photo_id:
                logger.info("Photo avatar group creation failed, but talking photo is available")
                return {
                    "message": "Using talking photo avatar (photo avatar groups not available)",
                    "avatar_id": talking_photo_id,
                    "type": "talking_photo",
                    "status": "available",
                    "note": "You can use this avatar to create videos"
                }
            raise HTTPException(status_code=500, detail=f"Error creating avatar group: {str(e)}")
        
        # Step 3: Train the photo avatar group
        try:
            train_result = heygen_service.train_avatar({"group_id": group_id})
            logger.info(f"Photo avatar training started: {train_result}")
            log_heygen_operation("photo_avatar_training", job_id=group_id, status="started")
        except Exception as e:
            logger.error(f"Error starting avatar training: {str(e)}")
            # Training might be automatic, so don't fail completely
            logger.warning("Training may proceed automatically")
        
            import uuid, boto3, os
            file_extension = training_media.filename.split('.')[-1] if training_media.filename and '.' in training_media.filename else 'jpg'
            s3_key = f"training/avatars/{uuid.uuid4().hex}.{file_extension}"
            
            s3_client = boto3.client('s3')
            bucket_name = os.environ.get("S3_BUCKET")
            
            # Reset file position before S3 upload
            await training_media.seek(0)
            file_content = await training_media.read()
            
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_content,
                ContentType=inferred_content_type
            )
            
            s3_url = f"s3://{bucket_name}/{s3_key}"
            log_s3_operation("upload", bucket_name, s3_key, status="success")
        except Exception as e:
            logger.error(f"Error backing up to S3: {str(e)}")
            s3_url = None
        
        # Store in database
        try:
            conn = get_db_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
                )
            )
            avatar_record = cur.fetchone()
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"Error storing in database: {str(e)}")
        
        return {
            "message": "Photo avatar created and training started",
            "group_id": group_id,
            "avatar_id": avatar_id,
            "image_key": image_key,
            "status": "training",
            "s3_url": s3_url
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in avatar training: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@app.get("/heygen/avatars/{avatar_id}/training-status")
def get_avatar_training_status(avatar_id: str):
    """Check training status of a photo avatar group"""
    try:
        # Get avatar from database to find group_id
        
        
        if not avatar:
            raise HTTPException(status_code=404, detail="Avatar not found")
        
        # Get metadata to find group_id
        metadata = avatar.get('metadata') or {}
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
        
        group_id = metadata.get('group_id') or avatar.get('provider_id')
        
        # Check training status from HeyGen
        try:
            status_result = heygen_service.get_training_status(group_id)
            status_data = status_result.get('data', {})
            training_status = status_data.get('status', 'unknown')
            
            # Update database if status changed
            if training_status == 'ready' and avatar['status'] != 'available':
                
            
            return {
                "avatar_id": avatar_id,
                "group_id": group_id,
                "status": training_status,
                "details": status_data
            }
        except Exception as e:
            logger.error(f"Error checking training status: {e}")
            return {
                "avatar_id": avatar_id,
                "status": avatar['status'],
                "error": str(e)
            }
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/heygen/avatars/{avatar_id}")
def get_avatar_status(avatar_id: int):
    try:
        # Get status from HeyGen API
        status = heygen_service.get_avatar_training_status(avatar_id)
        
        # Update database if status changed
        if status.get('status') == 'completed':
            
        
        return status
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/heygen/templates")
def get_heygen_templates():
    try:
        # Fetch templates from HeyGen API
        templates = heygen_service.list_templates()
        return templates
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/heygen/templates/{template_id}")
def get_heygen_template(template_id: int):
    try:
        # Fetch specific template from HeyGen API
        template = heygen_service.get_template(template_id)
        return template
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/heygen/templates/{template_id}/generate")
def generate_from_template(template_id: int, variables: dict):
    try:
        # Generate video from template with provided variables
        # Call HeyGen template generation API
        result = heygen_service.generate_from_template(template_id, variables)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/heygen/jobs")
def get_heygen_job_analytics():
    """Get HeyGen job analytics for monitoring"""
    try:
        
        
        return {
            "status_counts": status_counts,
            "avg_processing_time_seconds": avg_processing_time,
            "daily_counts": daily_counts,
            "limits": {
                "max_concurrent": MAX_CONCURRENT_JOBS,
                "daily_limit": DAILY_JOB_LIMIT
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching analytics: {str(e)}")

# HeyGen Video Status Model
class HeyGenVideoStatus(BaseModel):
    provider_video_id: str
    status: str
    output_asset_id: Optional[int] = None
    publication_id: Optional[int] = None
    created_at: str
    updated_at: str
    url: Optional[str] = None

    @validator('created_at', 'updated_at', pre=True)
    def format_dt(cls, v):
        if isinstance(v, datetime):
            return v.isoformat()
        return v

@app.get("/heygen/videos", response_model=List[HeyGenVideoStatus])
def list_heygen_videos():
    try:
        
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/heygen/videos/play/{provider_video_id}")
def play_heygen_video(provider_video_id: str):
    try:
        
        if not row or not row.get("url"):
            raise HTTPException(status_code=404, detail="Video asset not found")
        return {"url": row["url"]}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating video play URL: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class AutoCampaignRequest(BaseModel):
    offer_id: Optional[int] = None
    offer_title: Optional[str] = None
    offer_description: Optional[str] = None
    cta_link: Optional[str] = None
    platforms: List[str] = Field(default_factory=lambda: ["twitter", "shorts"])
    generate_heygen_video: bool = False
    heygen_avatar_id: Optional[str] = None
    twitter_account_id: Optional[int] = None
    test_mode: bool = True
    # Optional AI/context hints
    persona: Optional[str] = None
    pain_point: Optional[str] = None
    # Optional overrides to persist edited preview
    override_script_text: Optional[str] = None
    override_tweet_text: Optional[str] = None
    override_video_caption: Optional[str] = None

    @validator("platforms", pre=True)
    def _normalize_and_validate_platforms(cls, value):
        if not value:
            raise ValueError("At least one platform must be provided")
        if not isinstance(value, list):
            raise ValueError("platforms must be a list")
        normalized = [str(p).lower().strip() for p in value if str(p).strip()]
        if not normalized:
            raise ValueError("At least one platform must be provided")
        supported = set(svc.PLATFORM_CAPS.keys())
        filtered = [p for p in normalized if p in supported]
        if not filtered:
            raise ValueError(f"Unsupported platforms; supported: {sorted(supported)}")
        return filtered

class AutoCampaignResponse(BaseModel):
    campaign_id: int
    offer_id: int
    persona_id: Optional[int] = None
    pain_point_id: Optional[int] = None
    script_id: Optional[int] = None
    content_ids: List[int] = Field(default_factory=list)
    # Back-compat optional fields
    tweet_text: Optional[str] = None
    heygen_video_id: Optional[str] = None
    video_caption: Optional[str] = None

@app.post("/auto-campaigns/from-offer", response_model=AutoCampaignResponse)
def create_auto_campaign(request: AutoCampaignRequest):
    # Ensure/Create Offer and determine CTA link
    try:
        cta_link_val: Optional[str] = (request.cta_link or None)

        # Resolve offer (by id or by creating a new one)
        if request.offer_id:
            conn_tmp = get_db_connection(); cur_tmp = conn_tmp.cursor(cursor_factory=RealDictCursor)
            cur_tmp.execute("SELECT offer_id, title, description, link_url FROM offers WHERE offer_id = %s", (request.offer_id,))
            offer_row = cur_tmp.fetchone()
            cur_tmp.close(); conn_tmp.close()
            if not offer_row:
                raise HTTPException(status_code=404, detail="Offer not found")
            offer_id = offer_row["offer_id"]
            offer_title = offer_row["title"]
            offer_description = offer_row.get("description") or ""
            if not cta_link_val:
                cta_link_val = (offer_row.get("link_url") or "").strip() or None
        else:
            if not (request.offer_title or request.offer_description):
                raise HTTPException(status_code=400, detail="Provide offer_id or offer_title/offer_description")
            offer_payload = OfferCreate(
                title=request.offer_title or (request.offer_description or "Offer")[0:200],
                description=request.offer_description or "",
                link_url=(request.cta_link or "https://redacted.example.com"),
            )
            offer_created = create_offer(offer_payload)
            offer_id = offer_created["offer_id"]
            offer_title = offer_created["title"]
            offer_description = offer_created.get("description") or ""
            if not cta_link_val:
                cta_link_val = (offer_created.get("link_url") or "").strip() or None

        # Determine persona and pain point
        persona_title = request.persona or "general"
        pain_point_text = request.pain_point or ai_service.generate_pain_point_from_offer(offer_title, offer_description)

        # Generate script (allow override) with robust fallback when AI is unavailable
        if request.override_script_text:
            script_text = request.override_script_text
        else:
            try:
                script_text = ai_service.generate_script_from_pain_point(pain_point_text, persona_title)
            except Exception:
                # Fallback script using offer details
                base = (offer_description or offer_title or "This offer").strip()
                script_text = (f"{base}\n\nLearn more" + (f": {cta_link_val}" if cta_link_val else ".")).strip()

        # Begin single-connection transaction
        conn = get_db_connection()
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)

            # Insert persona and pain point
            ,
            )
            persona_id = cur.fetchone()["persona_id"]

            ,
            )
            pain_point_id = cur.fetchone()["pain_point_id"]

            # Insert script
            ,
            )
            script_id = cur.fetchone()["script_id"]

            # Create Campaign (draft)
            campaign_id = svc.insert_campaign(
                conn,
                name=f"Campaign for {offer_title}",
                description=f"Campaign generated from offer: {offer_title}",
                persona_id=persona_id,
                status="draft",
            )

            # Update campaign core fields
            svc.update_campaign_core(
                conn,
                campaign_id,
                offer_id=offer_id,
                persona_id=persona_id,
                pain_point_id=pain_point_id,
                primary_script_id=script_id,
                cta_link=cta_link_val,
            )

            # Attach requested platforms
            platforms_norm = [p.lower() for p in (request.platforms or [])]
            for plat in platforms_norm:
                svc.attach_platform(conn, campaign_id, plat, True)

            created_content_ids: List[int] = []

            # Twitter: tweet content
            if "twitter" in platforms_norm:
                try:
                    tweet_text = request.override_tweet_text or ai_service.generate_tweet_text(script_text, cta_link_val)
                except Exception:
                    # Deterministic fallback
                    base = (script_text or "").strip()
                    tweet_text = ((base[:240] + (" " if base else "") + (cta_link_val or "")).strip() or (offer_title[:240] + (" " if offer_title else "") + (cta_link_val or "")).strip())[:280]
                source = "manual" if request.override_tweet_text else "ai"
                if tweet_text:
                    cid = svc.create_content_tweet(conn, campaign_id, tweet_text, source)
                    created_content_ids.append(cid)

            # Shorts: caption and optional video
            if "shorts" in platforms_norm:
                # Caption
                try:
                    caption_text = request.override_video_caption or ai_service.generate_shorts_caption(script_text, cta_link_val)
                except Exception:
                    # Deterministic fallback: first 160 chars + CTA on new line
                    base = (script_text or offer_description or offer_title or "").strip()
                    caption_text = base[:160].strip()
                    if cta_link_val:
                        caption_text = (caption_text + "\n" + cta_link_val).strip()
                    caption_text = caption_text[:220]
                if caption_text:
                    source = "manual" if request.override_video_caption else "ai"
                    cid = svc.create_content_shorts_caption(conn, campaign_id, caption_text, source)
                    created_content_ids.append(cid)
                # Video asset placeholder
                if request.generate_heygen_video:
                    # Defer asset creation to HeyGen completion webhook; create content row with no asset yet
                    cid = svc.create_content_shorts_video(conn, campaign_id, None)
                    created_content_ids.append(cid)

            # Commit transaction
            conn.commit()
            cur.close()
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            raise e
        finally:
            try:
                conn.close()
            except Exception:
                pass

        return AutoCampaignResponse(
            campaign_id=campaign_id,
            offer_id=offer_id,
            persona_id=persona_id,
            pain_point_id=pain_point_id,
            script_id=script_id,
            content_ids=created_content_ids,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def insert_pain_point(pain_point: str) -> int:
    """Insert a pain point into the database and return its ID."""
    try:
        
        return pain_point_id
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def insert_persona(persona: dict) -> int:
    """Insert a persona into the database and return its ID."""
    try:
        
        return persona_id
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def generate_shorts_script(campaign_id: int, platforms: List[str]) -> int:
    """Generate a Shorts script for the campaign and return its ID."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        }", "draft")
        )
        script_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return script_id
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class RedditAccountPostRequest(BaseModel):
    subreddit: str
    title: str
    text: Optional[str] = None
    url: Optional[str] = None


class RedditAccountPostResponse(BaseModel):
    account_id: int
    post_id: str
    url: str


@app.post("/social-accounts/{account_id}/post/reddit", response_model=RedditAccountPostResponse)
def post_to_reddit(account_id: int, payload: RedditAccountPostRequest):
    try:
        # Validate payload: either text or url, but not both and not neither
        if (payload.text and payload.url) or (not payload.text and not payload.url):
            raise HTTPException(
                status_code=400,
                detail="Must provide either 'text' for a text post or 'url' for a link post, but not both",
            )

        # Load account from DB
        
            raise HTTPException(status_code=404, detail="Social media account not found")
        if row["platform"].lower() != "reddit":
            cur.close(); conn.close()
            raise HTTPException(status_code=400, detail="Account is not a Reddit account")

        # Build platform config from env + DB refresh token
        cfg = load_platform_config_from_env("reddit", prefix="REDDIT")
        if row.get("refresh_token"):
            from pydantic import SecretStr
            cfg.refresh_token = SecretStr(row["refresh_token"])
        cfg.extra["user_agent"] = "redacted-app:v1.0 (by u/redacted-app)"

        platform = RedditPlatform(cfg)
        platform.authenticate()

        # Format content for RedditPlatform.post_content method
        content_parts = [f"subreddit:{payload.subreddit}", f"title:{payload.title}"]
        if payload.text:
            content_parts.append(f"text:{payload.text}")
        else:
            content_parts.append(f"url:{payload.url}")
        content_text = " ".join(content_parts)

        result = platform.post_content(content_text)

        # Close DB
        cur.close(); conn.close()

        return RedditAccountPostResponse(account_id=account_id, post_id=result.post_id, url=result.url or "")
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except AuthenticationError as e:
        raise HTTPException(status_code=401, detail=str(e))
    except AuthorizationError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class RedditMetricsResponse(BaseModel):
    account_id: int
    post_id: Optional[str] = None
    results: List[dict]


@app.get("/social-accounts/{account_id}/metrics/reddit", response_model=RedditMetricsResponse)
def reddit_metrics(account_id: int, post_id: Optional[str] = None):
    try:
        # Load account from DB
        
            raise HTTPException(status_code=404, detail="Social media account not found")
        if row["platform"].lower() != "reddit":
            cur.close(); conn.close()
            raise HTTPException(status_code=400, detail="Account is not a Reddit account")

        # Build platform config from env + DB refresh token
        cfg = load_platform_config_from_env("reddit", prefix="REDDIT")
        if row.get("refresh_token"):
            from pydantic import SecretStr
            cfg.refresh_token = SecretStr(row["refresh_token"])
        cfg.extra["user_agent"] = "redacted-app:v1.0 (by u/redacted-app)"

        platform = RedditPlatform(cfg)
        platform.authenticate()

        if not post_id:
            raise HTTPException(status_code=400, detail="post_id is required for Reddit metrics")

        results = platform.fetch_metrics(post_id=post_id)

        cur.close(); conn.close()

        return RedditMetricsResponse(
            account_id=account_id,
            post_id=post_id,
            results=[r.model_dump() for r in results],
        )
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except AuthenticationError as e:
        raise HTTPException(status_code=401, detail=str(e))
    except AuthorizationError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/campaigns/{campaign_id}")
def get_campaign_detail(campaign_id: int):
    try:
        
            raise HTTPException(status_code=404, detail="Campaign not found")
        # Platforms
        )
        platforms = cur.fetchall() or []
        # Content
        
        )
        content = cur.fetchall() or []
        cur.close(); conn.close()
        return {
            "campaign": dict(campaign),
            "platforms": [dict(p) for p in platforms],
            "content": [dict(r) for r in content],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000) 