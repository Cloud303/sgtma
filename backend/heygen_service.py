# heygen_service.py

import requests
import json
import logging
from demo_flags import DEMO_MODE

logger = logging.getLogger(__name__)

class HeyGenService:
    def __init__(self, api_key):
        self.api_key = "REDACTED" if DEMO_MODE else api_key
        self.base_url = "https://redacted.example.com"

    def list_voices(self):
        """Get all available voices including custom ones"""
        if DEMO_MODE:
            return {"data": {"voices": []}}
        try:
            response = requests.get(f"{self.base_url}/voices", headers=self._headers())
            response.raise_for_status()
            data = response.json()
            logger.info(f"Listed {len(data.get('data', {}).get('voices', []))} voices")
            return data
        except Exception as e:
            logger.error(f"Error listing voices: {str(e)}")
            return {"error": str(e), "data": {"voices": []}}

    def list_avatars(self):
        """Get all available avatars including custom ones"""
        if DEMO_MODE:
            return {"data": {"avatars": [], "talking_photos": []}}
        try:
            response = requests.get(f"{self.base_url}/avatars", headers=self._headers())
            response.raise_for_status()
            data = response.json()
            logger.info(f"Listed {len(data.get('data', {}).get('avatars', []))} avatars")
            logger.info(f"Listed {len(data.get('data', {}).get('talking_photos', []))} talking photos")
            return data
        except Exception as e:
            logger.error(f"Error listing avatars: {str(e)}")
            return {"error": str(e), "data": {"avatars": [], "talking_photos": []}}

    def get_avatar(self, avatar_id):
        """Get specific avatar details"""
        if DEMO_MODE:
            return {"data": {}}
        response = requests.get(f"{self.base_url}/avatars/{avatar_id}", headers=self._headers())
        response.raise_for_status()
        return response.json()

    def get_voice(self, voice_id):
        """Get specific voice details"""
        if DEMO_MODE:
            return {"data": {}}
        response = requests.get(f"{self.base_url}/voices/{voice_id}", headers=self._headers())
        response.raise_for_status()
        return response.json()

    def generate_video(self, data):
        """Generate video with HeyGen"""
        if DEMO_MODE:
            raise RuntimeError("HeyGen write operations disabled in demo.")
        import time
        max_retries = 3
        # Construct payload
        # Check if using free tier (test mode required for free plan)
        is_free_tier = data.get("free_tier", True)
        
        # Check if this is a talking photo
        avatar_id = data.get("avatar_id")
        is_talking_photo = data.get("is_talking_photo", False) or (
            avatar_id and avatar_id.startswith("talking_photo_") 
        )
        
        # Build character/avatar config based on type
        video_input = {}
        if is_talking_photo:
            video_input["character"] = {
                "type": "talking_photo",
                "talking_photo_id": avatar_id
            }
        else:
            video_input["avatar"] = {
                "avatar_id": avatar_id,
                "avatar_style": data.get("avatar_style", "normal")
            }
        
        video_input["voice"] = {
            "type": "text",
            "voice_id": data.get("voice_id"),
            "input_text": data.get("input_text", "")
        }
        
        payload = {
            "video_inputs": [video_input],
            # CRITICAL: Set test=true for free tier to avoid resolution errors
            "test": data.get("test", is_free_tier),
            "caption": data.get("caption", False)
        }

        # Add dimension for free tier (360p max)
        if is_free_tier:
            payload["dimension"] = {
                "width": 640,
                "height": 360
            }

        # Add background if provided
        bg = data.get("background")
        if bg:
            if bg.startswith("http"):
                # Image background
                payload["video_inputs"][0]["background"] = {
                    "type": "image",
                    "source": {"type": "url", "url": bg}
                }
            else:
                # Color background
                payload["video_inputs"][0]["background"] = {"type": "color", "value": bg}
        else:
            # Default to white
            payload["video_inputs"][0]["background"] = {"type": "color", "value": "#FFFFFF"}

        # Add webhook URL if provided
        if data.get("webhook_url"):
            payload["webhook_url"] = data["webhook_url"]

        # Retry on failures (HTTP 5xx or network issues)
        for attempt in range(max_retries):
            try:
                logger.info(f"Generating video (attempt {attempt+1}) with payload: {json.dumps(payload, indent=2)}")
                response = requests.post(f"{self.base_url}/video/generate", json=payload, headers=self._headers())
                # Raise for 4xx/5xx
                response.raise_for_status()
                result = response.json()
                logger.info(f"Video generation started: {result}")
                return result
            except requests.exceptions.HTTPError as e:
                status = e.response.status_code if e.response is not None else None
                # Retry on server errors
                if status and status >= 500 and attempt < max_retries - 1:
                    logger.warning(f"HeyGen API server error {status}, retrying after backoff...")
                    time.sleep(2 ** attempt)
                    continue
                logger.error(f"Error generating video: {str(e)}")
                raise
            except Exception as e:
                # Network or other error
                if attempt < max_retries - 1:
                    logger.warning(f"Network error during video generation: {e}, retrying... ")
                    time.sleep(2 ** attempt)
                    continue
                logger.error(f"Error generating video: {str(e)}")
                raise

    def get_video_status(self, video_id):
        """Get video generation status"""
        if DEMO_MODE:
            return {"status": "unknown", "message": "Use webhook for status updates"}
        # Note: HeyGen v2 doesn't seem to have a direct status endpoint
        # Status is typically delivered via webhook
        logger.warning(f"Video status endpoint not available for video_id: {video_id}")
        return {"status": "unknown", "message": "Use webhook for status updates"}

    def create_talking_photo_avatar(self, avatar_data):
        """Create a talking photo avatar"""
        if DEMO_MODE:
            raise RuntimeError("HeyGen write operations disabled in demo.")
        # Note: The talking photo creation endpoint seems to be removed in v2
        # Talking photos are pre-made and listed in the avatars response
        logger.warning("Talking photo creation endpoint not available in v2 API")
        return {"error": "Talking photo creation not available in v2 API"}

    def list_templates(self):
        """Get available video templates"""
        if DEMO_MODE:
            return {"data": {"templates": []}}
        try:
            response = requests.get(f"{self.base_url}/templates", headers=self._headers())
            if response.status_code == 404:
                logger.info("Templates endpoint not available")
                return {"data": {"templates": []}}
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error listing templates: {str(e)}")
            return {"error": str(e), "data": {"templates": []}}

    def train_avatar(self, data: dict) -> dict:
        """Start training a photo avatar group.
        Expected data keys: group_id
        """
        if DEMO_MODE:
            raise RuntimeError("HeyGen write operations disabled in demo.")
        payload = {
            "group_id": data.get("group_id")
        }
        try:
            logger.info(f"Starting photo avatar group training with payload: {json.dumps(payload, indent=2)}")
            response = requests.post(f"{self.base_url}/photo_avatar/train", json=payload, headers=self._headers())
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HeyGen photo avatar training HTTP error: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response content: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"HeyGen photo avatar training error: {e}")
            raise

    def upload_photo(self, image_data: bytes, content_type: str = "image/jpeg") -> dict:
        """Upload a photo to HeyGen and get an image_key for avatar creation.
        Returns: {"image_key": "image/xxx/original", "image_url": "https://redacted.example.com"}
        """
        if DEMO_MODE:
            raise RuntimeError("HeyGen write operations disabled in demo.")
        try:
            logger.info(f"Uploading photo to HeyGen ({len(image_data)} bytes, type: {content_type})")
            headers = {
                "X-Api-Key": self.api_key,
                "Content-Type": content_type
            }
            # Try the standard upload endpoint first
            upload_url = "https://redacted.example.com"
            try:
                response = requests.post(upload_url, files={'file': ('photo.jpg', image_data, content_type)}, headers={"X-Api-Key": self.api_key})
                if response.status_code == 200:
                    result = response.json()
                    if result.get("code") == 100 and result.get("data"):
                        return {
                            "image_key": result["data"].get("url") or result["data"].get("key"),
                            "image_url": result["data"].get("url")
                        }
            except Exception as e:
                logger.warning(f"Standard upload failed, trying talking photo: {e}")
            
            # Fallback to talking photo endpoint
            upload_url = "https://redacted.example.com"
            response = requests.post(upload_url, data=image_data, headers=headers)
            response.raise_for_status()
            result = response.json()
            # Extract image key from response
            if result.get("code") == 100 and result.get("data"):
                data = result["data"]
                # Convert talking_photo response to image_key format
                return {
                    "image_key": f"image/{data.get('talking_photo_id')}/original",
                    "image_url": data.get("talking_photo_url"),
                    "talking_photo_id": data.get("talking_photo_id")
                }
            else:
                raise RuntimeError(f"Unexpected upload response: {result}")
        except Exception as e:
            logger.error(f"Photo upload error: {e}")
            raise
        
    def generate_photo_avatar_photos(self, image_url: str) -> dict:
        """Generate photo avatar photos from an uploaded image.
        This might be required before creating a photo avatar group.
        """
        if DEMO_MODE:
            raise RuntimeError("HeyGen write operations disabled in demo.")
        payload = {
            "image_url": image_url
        }
        try:
            logger.info(f"Generating photo avatar photos from: {image_url}")
            response = requests.post(
                f"{self.base_url}/photo_avatar/photo/generate",
                json=payload,
                headers=self._headers()
            )
            response.raise_for_status()
            result = response.json()
            logger.info(f"Photo generation response: {result}")
            return result
        except Exception as e:
            logger.error(f"Photo generation error: {e}")
            raise

    def create_photo_avatar_group(self, name: str, image_key: str) -> dict:
        """Create a photo avatar group with an uploaded image.
        Returns: {"group_id": "xxx", "id": "xxx", ...}
        """
        if DEMO_MODE:
            raise RuntimeError("HeyGen write operations disabled in demo.")
        payload = {
            "name": name,
            "image_key": image_key
        }
        try:
            logger.info(f"Creating photo avatar group: {name} with image_key: {image_key}")
            response = requests.post(
                f"{self.base_url}/photo_avatar/avatar_group/create", 
                json=payload, 
                headers=self._headers()
            )
            if response.status_code == 404:
                # Try without avatar_group in path
                logger.info("Trying alternate endpoint without avatar_group")
                response = requests.post(
                    f"{self.base_url}/photo_avatar/create", 
                    json=payload, 
                    headers=self._headers()
                )
            response.raise_for_status()
            result = response.json()
            logger.info(f"Create photo avatar group response: {result}")
            return result
        except requests.exceptions.HTTPError as e:
            logger.error(f"Photo avatar group creation HTTP error: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response content: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Photo avatar group creation error: {e}")
            raise

    def get_training_status(self, group_id: str) -> dict:
        """Get the training status of a photo avatar group."""
        if DEMO_MODE:
            return {"status": "unknown"}
        try:
            response = requests.get(
                f"{self.base_url}/photo_avatar/train/status/{group_id}",
                headers=self._headers()
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error getting training status: {e}")
            raise

    def _headers(self):
        return {
            "X-Api-Key": self.api_key,
            "Content-Type": "application/json"
        }
