"""MIT Learn API client for sending HMAC-signed webhook payloads."""

import hashlib
import hmac
import json
import os
from typing import Any

from pydantic import Field

from ol_orchestrate.resources.api_client import BaseApiClient


class MITLearnApiClient(BaseApiClient):
    """HTTP client for MIT Learn's signed webhook API.

    Wraps :class:`BaseApiClient` with HMAC-SHA256 request signing so that
    MIT Learn can verify the origin of every inbound webhook payload.
    """

    token: str = Field(
        description="secret key for HMAC signing of requests",
    )

    @classmethod
    def from_secret(cls, raw_secret: dict[str, Any]) -> "MITLearnApiClient":
        """Construct a client from a raw Vault secret dict."""
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
        """Send a ContentFile update webhook."""
        return self._post_signed_webhook("/api/v1/webhooks/content_files/", data)

    def notify_ovs_video(self, data: dict[str, Any]) -> dict[str, Any]:
        """Send webhook notification for an OVS include_in_learn video."""
        return self._post_signed_webhook("/api/v1/webhooks/ovs_videos/", data)

    # ------------------------------------------------------------------
    # REST API / dlt webhook delivery
    # Each method sends a batch of pre-computed LearningResource dicts.
    # MIT Learn handlers call load_courses() / load_programs() directly;
    # no transformation logic lives in the handler.
    # ------------------------------------------------------------------

    def notify_mit_climate(self, resources: list[dict[str, Any]]) -> dict[str, Any]:
        """Send pre-computed MIT Climate article resources to MIT Learn."""
        return self._post_signed_webhook(
            "/api/v1/webhooks/mit_climate/", {"resources": resources}
        )

    def notify_mitpe(self, resources: list[dict[str, Any]]) -> dict[str, Any]:
        """Send pre-computed MIT Professional Education resources to MIT Learn."""
        return self._post_signed_webhook(
            "/api/v1/webhooks/mitpe/", {"resources": resources}
        )

    def notify_oll(self, resources: list[dict[str, Any]]) -> dict[str, Any]:
        """Send pre-computed Open Learning Library resources to MIT Learn."""
        return self._post_signed_webhook(
            "/api/v1/webhooks/oll/", {"resources": resources}
        )

    def notify_mit_edx_programs(
        self, resources: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Send pre-computed MIT edX program resources to MIT Learn."""
        return self._post_signed_webhook(
            "/api/v1/webhooks/mit_edx_programs/", {"resources": resources}
        )
