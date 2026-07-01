# YouTube source

Loads MIT Learn YouTube data from the YouTube Data API v3 into the raw-data
layer, replacing the `learning_resources/etl/youtube.py` ETL in the MIT Learn
application. Wrapped as Dagster assets by the `data_loading` code location
(`data_loading/defs/ingestion/assets.py`).

**Config source:** [mitodl/open-video-data](https://github.com/mitodl/open-video-data)
(`youtube/*.yaml` — e.g. `channels.yaml`, `shorts.yaml` — on the `main` branch).
Each file is a **YAML list** of channel config dicts; add or edit a channel by
adding a `- channel_id: ...` entry. Each entry provides a `channel_id` and an
optional `playlists` list (entries may be `{id, ignore?}` dicts or bare id
strings). A channel with no `playlists`, or one containing the `all` wildcard id,
ingests every playlist on the channel.

```yaml
- channel_id: UCEBb1b_L6zDS3xTUrIALZOw
  offered_by: ocw
  playlists:
    - id: all
```

## Data flow

```
GitHub (mitodl/open-video-data/youtube/*.yaml)
    → channel_id + playlist configs per channel
    → YouTube Data API v3 (channels / playlists / playlistItems / videos)
        → raw__youtube__api__channels    (one row per configured channel)
        → raw__youtube__api__playlists   (one row per ingested playlist)
        → raw__youtube__api__videos      (one row per video)
    → youtube-transcript-api
        → raw__youtube__api__transcripts (one row per video with a transcript)
```

All tables use `write_disposition="merge"` keyed on the entity id so reruns
update in place.

## Implementation notes

- The Data API is called directly over HTTP with `requests` (API-key auth via
  the `key` query param), not the `google-api-python-client` SDK, to stay
  consistent with the other ol_dlt sources.
- Transcripts are **not** part of the Data API; they are fetched separately with
  `youtube-transcript-api` (coded against its 1.x `fetch()` instance API).
  Videos whose transcripts are disabled or unavailable are skipped (logged),
  not raised.

## Configuration

- **Secret:** `YOUTUBE_DEVELOPER_KEY` — YouTube Data API v3 key, resolved lazily
  at run time (`config.resolve_secret` / `config.require_secrets`).
- **Destination:** built by `ol_dlt.config.pipeline_for("youtube")` from the
  active `DLT_PROFILE` (no per-source `.dlt/config.toml` block).

## Standalone run

```bash
DLT_PROFILE=dev YOUTUBE_DEVELOPER_KEY=... python -m ol_dlt.sources.youtube
```
