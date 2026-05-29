"""
MIT Learn Podcast RSS ingestion via dlt.

Fetches podcast configuration from the mitodl/open-podcast-data GitHub repository,
then loads raw podcast channel and episode data from each configured RSS feed
into Iceberg tables for the raw data layer.

Data flow:
    GitHub (mitodl/open-podcast-data/podcasts/*.yaml)
        → RSS feed URLs
        → raw__podcast__channels  (one row per podcast)
        → raw__podcast__episodes  (one row per episode across all podcasts)

Usage (standalone):
    # Local development (default - writes to /tmp/.dlt/data/)
    python -m data_loading.defs.podcast_rss_ingest.loads

    # Production
    DAGSTER_ENVIRONMENT=production python -m data_loading.defs.podcast_rss_ingest.loads
"""

import base64
import logging
import os
from pathlib import Path
from xml.etree import ElementTree as ET

import dlt
import requests
import yaml

logger = logging.getLogger(__name__)

# Set dlt project directory to data_loading project root so .dlt/config.toml is found
_DLT_PROJECT_DIR = Path(__file__).parent.parent.parent.parent
if _DLT_PROJECT_DIR.exists():
    os.environ.setdefault("DLT_PROJECT_DIR", str(_DLT_PROJECT_DIR))

GITHUB_API_BASE = "https://api.github.com"
ITUNES_NS = "http://www.itunes.com/dtds/podcast-1.0.dtd"
# Browser-like UA to avoid RSS feed bot-blocking
_RSS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def _github_headers(token: str | None) -> dict[str, str]:
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _fetch_podcast_configs(
    repo: str,
    folder: str,
    branch: str,
    token: str | None,
) -> list[dict]:
    """
    Fetch and parse all podcast YAML config files from a GitHub repository.

    Returns a list of validated config dicts. Each dict must contain at least
    `rss_url` and `website` keys to be included.
    """
    headers = _github_headers(token)
    listing_url = f"{GITHUB_API_BASE}/repos/{repo}/contents/{folder}?ref={branch}"
    resp = requests.get(listing_url, headers=headers, timeout=30)
    resp.raise_for_status()

    configs = []
    for file_meta in resp.json():
        if not file_meta["name"].endswith(".yaml"):
            continue

        file_resp = requests.get(file_meta["url"], headers=headers, timeout=30)
        file_resp.raise_for_status()
        raw_content = base64.b64decode(file_resp.json()["content"]).decode("utf-8")

        try:
            config = yaml.safe_load(raw_content)
        except yaml.YAMLError:
            logger.exception("Failed to parse YAML config: %s", file_meta["name"])
            continue

        if not isinstance(config, dict):
            logger.warning("Skipping non-dict podcast config: %s", file_meta["name"])
            continue

        if "rss_url" not in config or "website" not in config:
            logger.warning(
                "Skipping config missing required keys (rss_url, website): %s",
                file_meta["name"],
            )
            continue

        configs.append(config)

    logger.info("Loaded %d podcast configs from %s/%s", len(configs), repo, folder)
    return configs


def _elem_text(parent: ET.Element, tag: str) -> str | None:
    """Return the text of the first matching child element, or None."""
    elem = parent.find(tag)
    return elem.text if elem is not None else None


def _parse_channel_record(channel_elem: ET.Element, config: dict) -> dict:
    """Extract channel-level fields from an RSS <channel> element."""
    rss_url = config["rss_url"]
    # Derive a stable readable_id from the URL path, matching Learn's existing convention
    readable_id = rss_url.split("//")[-1].rstrip("/")

    # Prefer iTunes namespace image; fall back to standard <image><url>
    itunes_image = channel_elem.find(f"{{{ITUNES_NS}}}image")
    if itunes_image is not None:
        image_url: str | None = itunes_image.get("href")
    else:
        img_elem = channel_elem.find("image")
        url_elem = img_elem.find("url") if img_elem is not None else None
        image_url = url_elem.text if url_elem is not None else None

    return {
        "readable_id": readable_id,
        "rss_url": rss_url,
        "website": config.get("website"),
        # YAML config may override the RSS channel title
        "title": config.get("podcast_title") or _elem_text(channel_elem, "title"),
        "description": _elem_text(channel_elem, "description"),
        "language": _elem_text(channel_elem, "language"),
        "last_build_date": _elem_text(channel_elem, "lastBuildDate"),
        "image_url": image_url,
        "offered_by": config.get("offered_by"),
        "topics": config.get("topics"),
        "apple_podcasts_url": config.get("apple_podcasts_url"),
        "google_podcasts_url": config.get("google_podcasts_url"),
        "etl_source": "podcast",
    }


def _parse_episode_record(
    item_elem: ET.Element,
    channel_readable_id: str,
    channel_rss_url: str,
    channel_image_url: str | None,
) -> dict | None:
    """
    Extract episode fields from an RSS <item> element.

    Returns None if the item has no <enclosure> (i.e. no audio file).
    """
    enclosure = item_elem.find("enclosure")
    if enclosure is None:
        return None
    audio_url = enclosure.get("url")
    if not audio_url:
        return None

    episode_link = _elem_text(item_elem, "link")
    guid = _elem_text(item_elem, "guid")
    # Use GUID as the stable readable_id; fall back to URL path
    readable_id = guid or (episode_link or audio_url).split("//")[-1]

    itunes_duration = item_elem.find(f"{{{ITUNES_NS}}}duration")
    itunes_image = item_elem.find(f"{{{ITUNES_NS}}}image")
    episode_image_url = (
        itunes_image.get("href") if itunes_image is not None else channel_image_url
    )

    return {
        "readable_id": readable_id,
        "channel_readable_id": channel_readable_id,
        "channel_rss_url": channel_rss_url,
        "title": _elem_text(item_elem, "title"),
        "description": _elem_text(item_elem, "description"),
        "url": episode_link or audio_url,
        "audio_url": audio_url,
        "episode_link": episode_link,
        "duration": itunes_duration.text if itunes_duration is not None else None,
        "pub_date": _elem_text(item_elem, "pubDate"),
        "image_url": episode_image_url,
        "etl_source": "podcast",
    }


@dlt.source
def podcast_rss_source(
    github_access_token: str | None = None,
    github_repo: str = "mitodl/open-podcast-data",
    github_folder: str = "podcasts",
    github_branch: str = "main",
):
    """
    Load MIT Learn podcast data from RSS feeds configured in GitHub.

    Reads YAML config files from the mitodl/open-podcast-data GitHub repository,
    fetches each configured RSS feed, and yields normalized records into two tables:

      raw__podcast__channels - one record per configured podcast channel
      raw__podcast__episodes - one record per episode across all channels

    Args:
        github_access_token: Optional GitHub personal access token. The public
            repository works without a token but is limited to 60 unauthenticated
            requests/hr. Set this to raise the limit to 5,000 req/hr.
        github_repo: GitHub repository containing the podcast YAML configs.
        github_folder: Folder within the repo containing YAML config files.
        github_branch: Git branch to read configs from.
    """

    @dlt.resource(
        name="raw__podcast__channels",
        write_disposition="merge",
        primary_key="readable_id",
    )
    def podcast_channels() -> None:
        """Yield one record per configured podcast channel."""
        configs = _fetch_podcast_configs(
            repo=github_repo,
            folder=github_folder,
            branch=github_branch,
            token=github_access_token,
        )
        for config in configs:
            rss_url = config["rss_url"]
            try:
                resp = requests.get(rss_url, headers=_RSS_HEADERS, timeout=30)
                resp.raise_for_status()
                root = ET.fromstring(resp.content)  # noqa: S314
            except requests.RequestException:
                logger.exception("Failed to fetch RSS feed: %s", rss_url)
                continue
            except ET.ParseError:
                logger.exception("Failed to parse RSS XML for: %s", rss_url)
                continue

            channel_elem = root.find("channel")
            if channel_elem is None:
                logger.warning("No <channel> element in RSS for: %s", rss_url)
                continue

            yield _parse_channel_record(channel_elem, config)

    @dlt.resource(
        name="raw__podcast__episodes",
        write_disposition="merge",
        primary_key="readable_id",
    )
    def podcast_episodes() -> None:
        """Yield one record per episode across all configured podcasts."""
        configs = _fetch_podcast_configs(
            repo=github_repo,
            folder=github_folder,
            branch=github_branch,
            token=github_access_token,
        )
        for config in configs:
            rss_url = config["rss_url"]
            try:
                resp = requests.get(rss_url, headers=_RSS_HEADERS, timeout=30)
                resp.raise_for_status()
                root = ET.fromstring(resp.content)  # noqa: S314
            except requests.RequestException:
                logger.exception("Failed to fetch RSS feed: %s", rss_url)
                continue
            except ET.ParseError:
                logger.exception("Failed to parse RSS XML for: %s", rss_url)
                continue

            channel_elem = root.find("channel")
            if channel_elem is None:
                continue

            channel_record = _parse_channel_record(channel_elem, config)
            for item_elem in channel_elem.findall("item"):
                episode = _parse_episode_record(
                    item_elem,
                    channel_readable_id=channel_record["readable_id"],
                    channel_rss_url=rss_url,
                    channel_image_url=channel_record["image_url"],
                )
                if episode is not None:
                    yield episode

    yield podcast_channels
    yield podcast_episodes


# ---------------------------------------------------------------------------
# Module-level source and pipeline instances referenced by defs.yaml
# ---------------------------------------------------------------------------

_dagster_env = os.getenv("DAGSTER_ENVIRONMENT", "dev")

if _dagster_env in ("qa", "production"):
    _destination_name = f"podcast_{_dagster_env}"
    _dataset_name = f"ol_warehouse_{_dagster_env}_raw"
else:
    _destination_name = "podcast_local"
    _dataset_name = "podcast_rss_local"

podcast_rss_load_source = podcast_rss_source()

podcast_rss_pipeline = dlt.pipeline(
    pipeline_name="podcast_rss",
    destination=_destination_name,
    dataset_name=_dataset_name,
    progress="log",
)


def _run_pipeline() -> None:
    """Execute the podcast RSS pipeline (for standalone testing)."""
    logging.basicConfig(level=logging.INFO)
    logger.info(
        "Running podcast RSS pipeline: destination=%s dataset=%s",
        _destination_name,
        _dataset_name,
    )
    load_info = podcast_rss_pipeline.run(podcast_rss_load_source)
    logger.info("Pipeline completed: %s", load_info)


if __name__ == "__main__":
    _run_pipeline()
