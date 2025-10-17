"""
Configuration provider for YouTube video shorts settings.
"""

import logging
from typing import Any

import requests
import yaml
from dagster import ConfigurableResource

log = logging.getLogger(__name__)


class YouTubeConfigProvider(ConfigurableResource):
    """
    Provides configuration for YouTube video processing.

    This resource handles fetching and parsing the configuration that defines
    which YouTube channels/playlists to monitor and process.
    """

    config_url: str = "https://raw.githubusercontent.com/mitodl/open-video-data/refs/heads/mitopen/youtube/shorts.yaml"

    def get_config(self) -> list[dict[str, Any]]:
        """
        Fetch and parse YouTube configuration.

        Returns:
            List of configuration items defining channels/playlists to process
        """
        try:
            response = requests.get(self.config_url, timeout=30)
            response.raise_for_status()
            config_data = yaml.safe_load(response.text)

            # Handle both single item and list formats
            if isinstance(config_data, list):
                return config_data
            else:
                return [config_data]

        except (requests.RequestException, yaml.YAMLError, KeyError) as e:
            # Fallback configuration for development/testing - using real MIT
            # Open Learning uploads
            fallback_config = [
                {
                    "name": "MIT Open Learning",
                    "type": "playlist",
                    # MIT Open Learning uploads playlist
                    "id": "UUSHN0QBfKk0ZSytyX_16M11fA",
                }
            ]
            # Use warning level but don't fail - fallback should work for testing
            log.warning(
                "Failed to fetch config from %s, using fallback: %s",
                self.config_url,
                e,
            )
            return fallback_config
