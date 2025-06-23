import hashlib
import hmac
import json
from typing import Any

from pydantic import Field

from ol_orchestrate.resources.api_client import BaseApiClient


class MITLearnApiClient(BaseApiClient):
    secret: str = Field(
        description="secret key for HMAC signing of requests",
    )

    def notify_course_export(self, data: dict[str, Any]) -> dict[str, Any]:
        payload_bytes = json.dumps(data).encode()
        signature = hmac.new(
            self.secret.encode(), payload_bytes, hashlib.sha256
        ).hexdigest()

        headers = {
            "X-MITLearn-Signature": signature,
            "Content-Type": "application/json",
        }

        response = self.http_client.post(
            f"{self.base_url}/webhooks/content_files/",
            content=payload_bytes,
            headers=headers,
        )
        response.raise_for_status()
        return response.json()
