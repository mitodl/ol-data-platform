{#-
    GENERATED FILE - DO NOT EDIT.

    Regenerate with `ol-dbt inventory history --write`; CI fails if this file and
    ingestion/inventory/units/podcast__rss.yml disagree.
-#}

{% snapshot snapshot_raw__podcast__rss__channels %}
{{ raw_history_snapshot(
    'raw__podcast__rss__channels',
    unique_key='readable_id',
    absence='aged_out',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__podcast__rss__episodes %}
{{ raw_history_snapshot(
    'raw__podcast__rss__episodes',
    unique_key='readable_id',
    absence='aged_out',
    live=false
) }}
{% endsnapshot %}
