{#-
    GENERATED FILE - DO NOT EDIT.

    Regenerate with `ol-dbt inventory history --write`; CI fails if this file and
    ingestion/inventory/units/mit_climate__api.yml disagree.
-#}

{% snapshot snapshot_raw__mit_climate__api__articles %}
{{ raw_history_snapshot(
    'raw__mit_climate__api__articles',
    unique_key='uuid',
    live=false
) }}
{% endsnapshot %}
