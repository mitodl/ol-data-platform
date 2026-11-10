{#-
    GENERATED FILE - DO NOT EDIT.

    Regenerate with `ol-dbt inventory history --write`; CI fails if this file and
    ingestion/inventory/units/mitpe__api.yml disagree.
-#}

{% snapshot snapshot_raw__mitpe__api__courses %}
{{ raw_history_snapshot(
    'raw__mitpe__api__courses',
    unique_key=['title', 'url'],
    live=false
) }}
{% endsnapshot %}
