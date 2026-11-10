{#-
    GENERATED FILE - DO NOT EDIT.

    Regenerate with `ol-dbt inventory history --write`; CI fails if this file and
    ingestion/inventory/units/ovs__app_postgres.yml disagree.
-#}

{% snapshot snapshot_raw__ovs__postgres__dj_elastictranscoder_encodejob %}
{{ raw_history_snapshot(
    'raw__ovs__postgres__dj_elastictranscoder_encodejob',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__ovs__postgres__ui_collection %}
{{ raw_history_snapshot(
    'raw__ovs__postgres__ui_collection',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__ovs__postgres__ui_collectionedxendpoint %}
{{ raw_history_snapshot(
    'raw__ovs__postgres__ui_collectionedxendpoint',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__ovs__postgres__ui_edxendpoint %}
{{ raw_history_snapshot(
    'raw__ovs__postgres__ui_edxendpoint',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__ovs__postgres__ui_encodejob %}
{{ raw_history_snapshot(
    'raw__ovs__postgres__ui_encodejob',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__ovs__postgres__ui_video %}
{{ raw_history_snapshot(
    'raw__ovs__postgres__ui_video',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}
