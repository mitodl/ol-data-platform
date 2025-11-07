"""YouTube API client resource for fetching video and playlist data."""

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, Self

from dagster import ConfigurableResource, InitResourceContext, ResourceDependency
from googleapiclient.discovery import Resource, build
from ol_orchestrate.resources.secrets.vault import Vault
from pydantic import Field, PrivateAttr

YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"


class YouTubeApiClient(ConfigurableResource):
    """Client for interacting with YouTube Data API v3."""

    api_key: str = Field(description="YouTube Data API v3 API key")
    _client: Resource | None = PrivateAttr(default=None)

    @property
    def client(self) -> Resource:
        """Get or create YouTube API client."""
        if not self._client:
            self._client = build(
                YOUTUBE_API_SERVICE_NAME,
                YOUTUBE_API_VERSION,
                developerKey=self.api_key,
            )
        return self._client

    def get_playlist_items(
        self, playlist_id: str, max_results: int = 50
    ) -> list[dict[str, Any]]:
        """
        Fetch all video IDs from a YouTube playlist.

        Args:
            playlist_id: The YouTube playlist ID
            max_results: Maximum results per page (default 50, max 50)

        Returns:
            List of playlist item dictionaries containing video information
        """
        items = []
        next_page_token = None

        while True:
            request = self.client.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=playlist_id,
                maxResults=max_results,
                pageToken=next_page_token,
            )
            response = request.execute()

            items.extend(response.get("items", []))
            next_page_token = response.get("nextPageToken")

            if not next_page_token:
                break

        return items

    def get_playlists(self, playlist_ids: list[str]) -> list[dict[str, Any]]:
        """Fetch metadata for multiple playlists by ID."""
        if not playlist_ids:
            return []

        playlists = []
        for i in range(0, len(playlist_ids), 50):
            batch = playlist_ids[i : i + 50]
            request = self.client.playlists().list(
                part="snippet,contentDetails",
                id=",".join(batch),
                maxResults=50,
            )
            response = request.execute()
            playlists.extend(response.get("items", []))

        return playlists

    def get_videos(self, video_ids: list[str]) -> list[dict[str, Any]]:
        """
        Fetch detailed information for multiple videos.

        Args:
            video_ids: List of YouTube video IDs

        Returns:
            List of video dictionaries with full metadata
        """
        if not video_ids:
            return []

        # API accepts up to 50 IDs per request
        videos = []
        for i in range(0, len(video_ids), 50):
            batch = video_ids[i : i + 50]
            request = self.client.videos().list(
                part="snippet,contentDetails,statistics",
                id=",".join(batch),
            )
            response = request.execute()
            videos.extend(response.get("items", []))

        return videos


class YouTubeApiClientFactory(ConfigurableResource):
    """Factory for creating YouTube API client with credentials from Vault."""

    vault: ResourceDependency[Vault]
    _youtube_client: YouTubeApiClient | None = PrivateAttr(default=None)

    def _initialize_client(self) -> YouTubeApiClient:
        """Initialize YouTube API client with API key from Vault."""
        secrets = self.vault.client.secrets.kv.v2.read_secret(
            mount_point="secret-mitlearn",
            path="secrets",
        )["data"]["data"]
        api_key = secrets["youtube_developer_key"]

        return YouTubeApiClient(api_key=api_key)

    @property
    def client(self) -> YouTubeApiClient:
        """Get or create YouTube API client."""
        if not self._youtube_client:
            self._youtube_client = self._initialize_client()
        return self._youtube_client

    @contextmanager
    def yield_for_execution(self, context: InitResourceContext) -> Generator[Self]:  # noqa: ARG002
        """Context manager for resource execution."""
        yield self
