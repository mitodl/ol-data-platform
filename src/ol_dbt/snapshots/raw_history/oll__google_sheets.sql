{#-
    GENERATED FILE - DO NOT EDIT.

    Regenerate with `ol-dbt inventory history --write`; CI fails if this file and
    ingestion/inventory/units/oll__google_sheets.yml disagree.
-#}

{% snapshot snapshot_raw__oll__google_sheets__courses %}
{{ raw_history_snapshot(
    'raw__oll__google_sheets__courses',
    unique_key='readable_id',
    live=false
) }}
{% endsnapshot %}
