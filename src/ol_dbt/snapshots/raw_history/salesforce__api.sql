{#-
    GENERATED FILE - DO NOT EDIT.

    Regenerate with `ol-dbt inventory history --write`; CI fails if this file and
    ingestion/inventory/units/salesforce__api.yml disagree.
-#}

{% snapshot snapshot_raw__thirdparty__salesforce___destination_v2__Opportunity %}
{{ raw_history_snapshot(
    'raw__thirdparty__salesforce___destination_v2__Opportunity',
    unique_key='Id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__thirdparty__salesforce___destination_v2__OpportunityLineItem %}
{{ raw_history_snapshot(
    'raw__thirdparty__salesforce___destination_v2__OpportunityLineItem',
    unique_key='Id',
    live=false
) }}
{% endsnapshot %}
