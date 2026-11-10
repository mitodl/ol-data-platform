{#-
    GENERATED FILE - DO NOT EDIT.

    Regenerate with `ol-dbt inventory history --write`; CI fails if this file and
    ingestion/inventory/units/edxorg__google_sheets.yml disagree.
-#}

{% snapshot snapshot_raw__edxorg__program_entitlement %}
{{ raw_history_snapshot(
    'raw__edxorg__program_entitlement',
    unique_key=['Email', 'Program Title'],
    live=false
) }}
{% endsnapshot %}
