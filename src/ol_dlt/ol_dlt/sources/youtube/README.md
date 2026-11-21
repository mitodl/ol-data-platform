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
        → raw__youtube__api__playlist_items (one row per playlist + video)
        → raw__youtube__api__videos      (one row per video)
    → YouTube Data API v3 captions (captions.list / captions.download, OAuth)
        → raw__youtube__api__transcripts (one row per caption track of a video, keyed
  by video_id, language_code and is_generated, and one status row per video with
  no language saying how its listing went; only videos OCW has no transcript for)
```

All tables use `write_disposition="merge"` keyed on the entity id (playlist
items on `playlist_id` + `video_id`, transcripts on `video_id` +
`language_code` + `is_generated`) so reruns update in place. Merge never
deletes, so a playlist, video or membership that disappears keeps its last
`_dlt_load_id`; the dbt integrations models filter on load recency for that.

Playlist rows carry the channel's `offered_by` and a resolved `create_videos`
flag, using MIT Learn's precedence: a playlist listed by id takes its own
`create_videos`, else the channel's, which defaults to true. A playlist reached
only through `all` takes the channel's.

## Implementation notes

- A deselected `youtube_listing` resource reads the config and lists every
  playlist and its items once per extraction. The table resources are dlt
  transformers of it, so a run makes one listing pass against the API key's
  daily quota (shared with MIT Learn), and every table sees the same snapshot.
- A run that would write no channels, playlists, playlist items or videos
  fails instead. Merge loading writes no load id for an empty table, so the
  previous snapshot would stay current downstream.
- A config file that doesn't parse, or a channel entry with no `channel_id`,
  fails the run. Skipping it would load a short channel set that the dbt
  integrations models read as complete.

- The Data API is called directly over HTTP with `requests`, not the
  `google-api-python-client` SDK, to stay consistent with the other ol_dlt
  sources. Channels, playlists and videos are public data, read with the API
  key (the `key` query param).
- Transcripts are caption tracks. `captions.list` and `captions.download` need
  OAuth (scope `youtube.force-ssl`), and a download "requires the user to have
  permission to edit the video", so they are read as a Google account the
  channels add as a manager. A service account cannot do it: "YouTube does not
  support Service Accounts" (the `youtubeSignupRequired` entry of the [Data API
  errors](https://developers.google.com/youtube/v3/docs/errors)). A video on a
  channel the account does not manage gets `fetch_status` `forbidden`.
- `captions.list` costs 50 quota units and `captions.download` 200, so a run
  spends at most `caption_quota_units` (default 9,000, under the 10,000 a day
  a project gets by default) and covers a few dozen videos. It skips videos OCW
  has transcript text for (`int__ocw__content_files`, read from the dbt build),
  then lists videos never listed, those with uploaded captions
  (`contentDetails.caption`) first, then those listed more than 90 days ago.
  The resource state keeps when each video was listed and the `lastUpdated` of
  each track downloaded, so an unchanged track is not downloaded again. When
  the quota runs out (`quotaExceeded`), the run stops.
- The `transcript_tracks` source option picks the tracks: `native` (default)
  downloads every uploaded track and the auto-generated (ASR) track in the
  video's language (from the `videos.list` snippet), `all` every serving,
  non-draft track. Tracks are downloaded as SRT; `segments` holds the cues and
  `transcript` their text. Each video listed gets a status row (empty
  `language_code`) with `fetch_status` (`fetched`, `none`, `unavailable`,
  `forbidden`, `failed`), any errors and `listed_tracks`. A failed download
  writes no row of its own, so the last good text stays.

## Configuration

- **API key:** YouTube Data API v3 key, resolved lazily at run time. The `qa`
  and `production` profiles read `developer_key` from Vault at
  `secret-data/pipelines/youtube` (written by the dagster stack in
  ol-infrastructure from MIT Learn's SOPS secrets). Other profiles read
  `YOUTUBE_DEVELOPER_KEY` from the environment.
- **Caption credentials:** an OAuth client id and secret and a refresh token,
  as `captions_client_id`, `captions_client_secret` and
  `captions_refresh_token` in the same Vault secret, or
  `YOUTUBE_CAPTIONS_CLIENT_ID`, `YOUTUBE_CAPTIONS_CLIENT_SECRET` and
  `YOUTUBE_CAPTIONS_REFRESH_TOKEN` outside the deployed profiles. Without them
  the transcripts table is not loaded. The OAuth client is made by hand in
  `mitol01` (Google has no API for OAuth clients; see ol-infrastructure's
  `src/ol_infrastructure/infrastructure/gcp/README.md`). An External consent
  screen in Testing status issues refresh tokens that expire in 7 days, so the
  client's consent screen is Internal (an mit.edu account) or published. The
  refresh token comes from one consent by the managing account with
  `access_type=offline` (Google's [installed app
  flow](https://developers.google.com/youtube/v3/guides/auth/installed-apps)),
  and stops working if it goes unused for six months or the account revokes it.
- **Destination:** built by `ol_dlt.config.pipeline_for("youtube")` from the
  active `DLT_PROFILE` (no per-source `.dlt/config.toml` block).

## Standalone run

```bash
DLT_PROFILE=dev YOUTUBE_DEVELOPER_KEY=... python -m ol_dlt.sources.youtube
```
