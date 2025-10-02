import boto3
import json
from typing import Optional, Dict, Any
import os
from datetime import datetime, timedelta
from demo_flags import DEMO_MODE

class AIService:
    def __init__(self):
        # Preserve structural attributes, remove embedded credentials
        self.region = None
        self.bedrock = None
        self.credentials = None
        self.credentials_expiry = None
        
        # Do not initialize external clients in open-source demo
        if not DEMO_MODE:
            # Intentionally disabled without real environment setup
            raise RuntimeError("Bedrock client initialization disabled in demo/open-source distribution.")
        
    def _initialize_bedrock_client(self):
        """Initialize the Bedrock client (disabled in demo)."""
        if DEMO_MODE:
            return
        raise RuntimeError("Disabled in open-source demo (no credentials).")
    
    def _check_bedrock_available(self):
        """Check if Bedrock is available before making calls."""
        if self.bedrock is None:
            raise Exception("AWS Bedrock not available. Please check credentials and permissions.")
        
    def generate_script_from_pain_point(self, pain_point: str, persona: str = None) -> str:
        """Generate a marketing script based on a pain point and optionally a persona."""
        
        self._check_bedrock_available()
        
        prompt = ""
        
        return self._call_bedrock(prompt)
    
    def generate_script_from_offer_pain_point_persona(self, offer: str, pain_point: str, persona: str) -> str:
        """Generate a marketing script based on a specific offer, pain point, and persona."""
        
        self._check_bedrock_available()
        
        prompt = ""
        
        return self._call_bedrock(prompt)
    
    def generate_pain_points_from_persona(self, persona: str) -> list:
        """Generate pain points based on a persona description."""
        
        self._check_bedrock_available()
        
        prompt = ""
        
        response = self._call_bedrock(prompt)
        try:
            return json.loads(response)
        except:
            lines = response.strip().split('\n')
            return [line.strip('- ').strip() for line in lines if line.strip()]
    
    def generate_offer_from_pain_point(self, pain_point: str, persona: str = None) -> Dict[str, str]:
        """Generate a marketing offer based on a pain point."""
        
        self._check_bedrock_available()
        
        prompt = ""
        
        response = self._call_bedrock(prompt)
        try:
            return json.loads(response)
        except:
            return {
                "title": f"Free {pain_point.split()[0]} Assessment",
                "description": f"Get a free assessment to solve your {pain_point.lower()} challenges",
                "cta_text": "Get Free Assessment",
                "link_url": "https://redacted.example.com"
            }
    
    def generate_persona_description(self, title: str, industry: str = None) -> str:
        """Generate a detailed persona description based on title and industry."""
        
        self._check_bedrock_available()
        
        prompt = ""
        
        return self._call_bedrock(prompt)
    
    def generate_content_ideas(self, topic: str, platform: str = "LinkedIn") -> list:
        """Generate content ideas for a given topic and platform."""
        
        self._check_bedrock_available()
        
        prompt = ""
        
        response = self._call_bedrock(prompt)
        try:
            return json.loads(response)
        except:
            return [
                {
                    "title": f"5 Ways to Improve {topic}",
                    "description": f"Learn the top strategies for {topic.lower()}",
                    "talking_points": [f"Strategy 1 for {topic}", f"Strategy 2 for {topic}"],
                    "hashtags": [f"#{topic.replace(' ', '')}", "#MarketingTips"]
                }
            ]
    
    # ============================================================================
    # CAMPAIGN MANAGEMENT AI FUNCTIONS (Phase 3.1)
    # ============================================================================
    
    def generate_cta_variations(self, campaign_description: str, platforms: list) -> list:
        """Generate platform-specific CTA variations for a campaign."""
        
        self._check_bedrock_available()
        
        platforms_text = ", ".join(platforms)
        prompt = ""
        
        response = self._call_bedrock(prompt)
        try:
            return json.loads(response)
        except:
            # Fallback response
            return [
                {
                    "platform": platform,
                    "ctas": [
                        {
                            "text": "Learn More",
                            "type": "link",
                            "url": "https://redacted.example.com",
                            "description": f"Standard CTA for {platform}"
                        },
                        {
                            "text": f"#{campaign_description.replace(' ', '')}",
                            "type": "hashtag",
                            "description": f"App hashtag for {platform}"
                        }
                    ]
                }
                for platform in platforms
            ]
    
    def generate_campaign_content(self, campaign_name: str, objective: str, target_persona: str, platforms: list) -> dict:
        """Generate comprehensive campaign content including CTAs and platform variations."""
        
        self._check_bedrock_available()
        
        platforms_text = ", ".join(platforms)
        prompt = ""
        
        response = self._call_bedrock(prompt)
        try:
            return json.loads(response)
        except:
            # Fallback response
            return {
                "campaign_name": campaign_name,
                "objective": objective,
                "target_persona": target_persona,
                "platforms": platforms,
                "content_strategy": {
                    "main_message": f"Campaign focused on {objective.lower()}",
                    "key_themes": ["Innovation", "Growth", "Success"],
                    "hashtags": [f"#{campaign_name.replace(' ', '')}", "#RedactedApp"]
                },
                "platform_content": [
                    {
                        "platform": platform,
                        "content_variations": [
                            {
                                "title": f"{campaign_name} on {platform}",
                                "content": f"Platform-specific content for {platform}",
                                "cta": "Learn More",
                                "hashtags": [f"#{platform}", "#RedactedApp"]
                            }
                        ]
                    }
                    for platform in platforms
                ],
                "ctas": [
                    {
                        "text": "Learn More",
                        "type": "link",
                        "platform": platform,
                        "description": f"Standard CTA for {platform}"
                    }
                    for platform in platforms
                ]
            }
    
    def generate_platform_variations(self, script_content: str, platforms: list) -> dict:
        """Generate platform-specific content variations from a base script."""
        
        self._check_bedrock_available()
        
        platforms_text = ", ".join(platforms)
        prompt = ""
        
        response = self._call_bedrock(prompt)
        try:
            return json.loads(response)
        except:
            # Fallback response
            return {
                "original_content": script_content,
                "platform_variations": [
                    {
                        "platform": platform,
                        "content": f"{script_content[:100]}... (optimized for {platform})",
                        "character_count": len(script_content),
                        "format_notes": f"Optimized for {platform} format",
                        "cta_suggestion": "Learn More"
                    }
                    for platform in platforms
                ]
            }
    
    def generate_pain_point_from_offer(self, title: str, description: str) -> str:
        """Generate a primary pain point from an offer's title and description.
        Returns a concise pain point string. Falls back to a sensible default when AI is unavailable.
        """
        prompt = ""
        try:
            ai_text = self._call_bedrock(prompt)
            data = json.loads(ai_text)
            pain = (data or {}).get('pain_point', '').strip()
            if pain:
                return pain
        except Exception:
            pass
        # Fallback
        base = title.strip() or 'your current marketing'
        return f"Struggling to get results from {base}"

    def suggest_persona_for_offer(self, title: str, description: str) -> dict:
        """Suggest a persona for an offer based on its title and description.
        Returns a dict with keys: title, description. Falls back when AI is unavailable.
        """
        prompt = ""
        try:
            ai_text = self._call_bedrock(prompt)
            data = json.loads(ai_text)
            persona_title = (data or {}).get('persona_title', '').strip() or 'Generic Persona'
            persona_desc = (data or {}).get('persona_description', '').strip() or 'No description available'
            return {"title": persona_title, "description": persona_desc}
        except Exception:
            # Fallback
            return {
                "title": "Content Manager",
                "description": "Time-strapped content manager at a growing SMB seeking faster, consistent content that drives measurable results."
            }

    def generate_video_caption(self, script_text: str, cta_link: str) -> str:
        """Generate a concise video caption (1-2 sentences) that includes the CTA link.
        Ensures the CTA link is present and returns a short fallback if AI is unavailable.
        """
        prompt = ""
        try:
            text = (self._call_bedrock(prompt) or '').strip()
            if cta_link not in text:
                base = text[: max(0, 160 - 1 - len(cta_link))].rstrip()
                text = (base + (' ' if base else '') + cta_link).strip()
            return text or f"Watch how to solve this in 60s. Learn more: {cta_link}"
        except Exception:
            return f"Watch how to solve this in 60s. Learn more: {cta_link}"

    def generate_tweet(self, script_summary: str, cta_link: str) -> str:
        """Generate a tweet (<= 280 chars) that includes the CTA link.
        Ensures the CTA link is included and enforces the character limit with graceful truncation.
        """
        max_len = 280
        prompt = ""
        try:
            text = (self._call_bedrock(prompt) or '').strip()
        except Exception:
            text = f"{script_summary[:180].rstrip(' .')} — Learn more: {cta_link}"
        # Ensure link presence and enforce limit
        if cta_link not in text:
            available = max_len - 1 - len(cta_link)
            base = text[: max(0, available)].rstrip()
            text = (base + (' ' if base else '') + cta_link).strip()
        # Final hard cap
        return text[:max_len].rstrip()
    
    def _call_bedrock(self, prompt: str) -> str:
        """Make a call to AWS Bedrock with Nova Pro."""
        
        # In demo mode, return blank output and do not call external services
        if DEMO_MODE:
            return ""
        
        if self.bedrock is None:
            return "AI service not available. Please check credentials and permissions."
        
        try:
            # Nova Pro request format - send empty messages to avoid publishing prompts
            body = json.dumps({
                "messages": []
            })
            
            response = self.bedrock.invoke_model(
                modelId="amazon.nova-pro-v1:0",
                body=body
            )
            
            response_body = json.loads(response.get('body').read())
            return response_body['output']['message']['content'][0]['text']
            
        except Exception as e:
            print(f"Error calling Bedrock: {e}")
            return f"AI service error: {str(e)}"

    def generate_tweet_text(self, seed_text: str, cta_link: Optional[str] = None) -> str:
        base = (seed_text or "").strip()
        if cta_link:
            text = f"{base[:240]} {cta_link}".strip()
        else:
            text = base[:280]
        return text[:280]

    def generate_shorts_caption(self, script_text: str, cta_link: Optional[str] = None) -> str:
        base = (script_text or "").strip()
        caption = base[:160].strip()
        if cta_link:
            caption = (caption + "\n" + cta_link).strip()
        return caption[:220]

# Create a singleton instance
ai_service = AIService()