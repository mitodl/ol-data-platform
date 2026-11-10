{#-
    GENERATED FILE - DO NOT EDIT.

    Regenerate with `ol-dbt inventory history --write`; CI fails if this file and
    ingestion/inventory/units/ocw__app_postgres.yml disagree.
-#}

{% snapshot snapshot_raw__ocw__studio__postgres__gdrive_sync_drivefile %}
{{ raw_history_snapshot(
    'raw__ocw__studio__postgres__gdrive_sync_drivefile',
    unique_key='file_id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__ocw__studio__postgres__websites_website %}
{{ raw_history_snapshot(
    'raw__ocw__studio__postgres__websites_website',
    unique_key='uuid',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__ocw__studio__postgres__websites_websitecontent %}
{{ raw_history_snapshot(
    'raw__ocw__studio__postgres__websites_websitecontent',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__ocw__studio__postgres__websites_websitestarter %}
{{ raw_history_snapshot(
    'raw__ocw__studio__postgres__websites_websitestarter',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}
