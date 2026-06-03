import hashlib
import hmac
import json
import os
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
        # Allow local development to point the client at a non-prod MIT Learn
        # without rewriting Vault.
        if override := os.environ.get("MIT_LEARN_BASE_URL"):
            learn["base_url"] = override
        if token_override := os.environ.get("MIT_LEARN_WEBHOOK_SECRET"):
            learn["token"] = token_override
        return cls(**learn)

    def _post_signed_webhook(self, path: str, data: dict[str, Any]) -> dict[str, Any]:
        payload_string = json.dumps(data, separators=(",", ":"))  # remove extra spaces
        signature = hmac.new(
            self.token.encode(), payload_string.encode(), hashlib.sha256
        ).hexdigest()
        headers = {
            "X-MITLearn-Signature": signature,
            "Content-Type": "application/json",
        }
        response = self.http_client.post(
            f"{self.base_url}{path}",
            content=payload_string,
            headers=headers,
        )
        response.raise_for_status()
        return response.json()

    def notify_course_export(self, data: dict[str, Any]) -> dict[str, Any]:
        return self._post_signed_webhook("/api/v1/webhooks/content_files/", data)

    def notify_ovs_video(self, data: dict[str, Any]) -> dict[str, Any]:
        """Send webhook notification for an OVS include_in_learn video."""
        return self._post_signed_webhook("/api/v1/webhooks/ovs_videos/", data)
