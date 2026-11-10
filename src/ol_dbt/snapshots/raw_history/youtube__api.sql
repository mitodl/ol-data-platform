{#-
    GENERATED FILE - DO NOT EDIT.

    Regenerate with `ol-dbt inventory history --write`; CI fails if this file and
    ingestion/inventory/units/youtube__api.yml disagree.
-#}

{% snapshot snapshot_raw__youtube__api__channels %}
{{ raw_history_snapshot(
    'raw__youtube__api__channels',
    unique_key='channel_id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__youtube__api__playlist_items %}
{{ raw_history_snapshot(
    'raw__youtube__api__playlist_items',
    unique_key=['playlist_id', 'video_id'],
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__youtube__api__playlists %}
{{ raw_history_snapshot(
    'raw__youtube__api__playlists',
    unique_key='playlist_id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__youtube__api__videos %}
{{ raw_history_snapshot(
    'raw__youtube__api__videos',
    unique_key='video_id',
    live=false
) }}
{% endsnapshot %}
