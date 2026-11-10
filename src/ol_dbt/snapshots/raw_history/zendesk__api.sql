{#-
    GENERATED FILE - DO NOT EDIT.

    Regenerate with `ol-dbt inventory history --write`; CI fails if this file and
    ingestion/inventory/units/zendesk__api.yml disagree.
-#}

{% snapshot snapshot_raw__thirdparty__zendesk_support__brands %}
{{ raw_history_snapshot(
    'raw__thirdparty__zendesk_support__brands',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__thirdparty__zendesk_support__organizations %}
{{ raw_history_snapshot(
    'raw__thirdparty__zendesk_support__organizations',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__thirdparty__zendesk_support__users %}
{{ raw_history_snapshot(
    'raw__thirdparty__zendesk_support__users',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}
