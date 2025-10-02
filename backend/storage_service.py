# storage_service.py

import boto3, mimetypes, requests, os, datetime, uuid
from demo_flags import DEMO_MODE

_s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION")) if not DEMO_MODE else None
_BUCKET = os.environ.get("S3_BUCKET", "REDACTED_BUCKET")
_PREFIX = os.environ.get("S3_PREFIX", "")

class StorageService:
    def __init__(self):
        pass

    def download_to_s3(self, url, kind, ext, content_type=None):
        key = self._generate_key(kind, ext)
        if DEMO_MODE:
            # Do not perform network or AWS operations in demo mode
            return {"bucket": "REDACTED_BUCKET", "key": key, "s3_url": f"s3://REDACTED_BUCKET/{key}"}
        with requests.get(url, stream=True, timeout=300) as r:
            r.raise_for_status()
            ct = content_type or r.headers.get("Content-Type") or mimetypes.guess_type(f"x.{ext}")[0] or "application/octet-stream"
            _s3.upload_fileobj(r.raw, _BUCKET, key, ExtraArgs={"ContentType": ct})
        return {"bucket": _BUCKET, "key": key, "s3_url": f"s3://{_BUCKET}/{key}"}

    def _generate_key(self, subdir, ext):
        d = datetime.datetime.utcnow()
        return f"{_PREFIX}/{subdir}/{d:%Y/%m/%d}/{uuid.uuid4().hex}.{ext}"

    def generate_signed_url(self, key, expires_in=3600):
        if DEMO_MODE:
            return f"https://redacted.example.com/{key}?demo=1&expires={expires_in}"
        return _s3.generate_presigned_url('get_object', Params={'Bucket': _BUCKET, 'Key': key}, ExpiresIn=expires_in) 