import hashlib
import hmac
import json
from typing import Any

from pydantic import Field

from ol_orchestrate.resources.api_client import BaseApiClient


class MITLearnApiClient(BaseApiClient):
    token: str = Field(
        description="secret key for HMAC signing of requests",
    )

    @classmethod
    def from_secret(cls, raw_secret: dict[str, Any]) -> "MITLearnApiClient":
        learn = raw_secret.get("learn") or {}
        learn["base_url"] = learn.get("base_url") or learn.pop("url", None)
        return cls(**learn)

    def notify_course_export(self, data: dict[str, Any]) -> dict[str, Any]:
        payload_string = json.dumps(data, separators=(",", ":"))  # remove extra spaces
        signature = hmac.new(
            self.token.encode(), payload_string.encode(), hashlib.sha256
        ).hexdigest()

        headers = {
            "X-MITLearn-Signature": signature,
        }

        response = self.http_client.post(
            f"{self.base_url}/webhooks/content_files/",
            content=payload_string,
            headers=headers,
        )
        response.raise_for_status()
        return response.json()
