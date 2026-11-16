-- MITx Online user activities from tracking logs
-- Due to size of the raw table, build this model as incremental and apply dedup on new rows
{{ config(
    materialized="incremental",
    unique_key=['user_username', 'useractivity_context_object', 'useractivity_event_source',
    'useractivity_event_type', 'useractivity_event_object', 'useractivity_timestamp'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    views_enabled=false
) }}

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__tracking_log_events') }}
    where
        (
            (username != '' and {{ json_query_string('context', "'$.user_id'") }} != 'null')
            -- MITx Online enrolls learners through the edX API, so these events carry the learner only in event.user_id
            or (username = '' and event_type like 'edx.course.enrollment.%')
        )
        and {{ json_query_string('event', "'$.exception'") }} is null

    {% if is_incremental() %}
        and {{ tracking_log_load_watermark() }} --noqa

        -- load the API enrollments from before this model kept them, without a full refresh.
        -- a separate scan with a literal bound is fast; an "or" in the filter above made each run 4x slower
        {% set api_enrollment_start = run_query(
            "select coalesce(min(useractivity_timestamp), '9999') from " ~ this ~ " where user_username = ''"
        ).columns[0].values()[0] %}
        union all
        select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__tracking_log_events') }}
        where
            username = '' and event_type like 'edx.course.enrollment.%'
            and {{ json_query_string('event', "'$.exception'") }} is null
            and {{ adapter.quote('time') }} < '{{ api_enrollment_start }}' --noqa
    {% endif %}
)

{{ deduplicate_raw_table(
    raw_table='raw__mitxonline__openedx__tracking_log_events'
    , partition_columns = 'username, context, event_source, event_type, event, ' ~ adapter.quote('time')
) }}
, cleaned as (
    select
        username as user_username
        , context as useractivity_context_object
        , event as useractivity_event_object
        , event_source as useractivity_event_source
        , page as useractivity_page_url
        , session as useractivity_session_id
        , ip as useractivity_ip
        , host as useractivity_http_host
        , agent as useractivity_http_user_agent
        , accept_language as useractivity_http_accept_language
        , referer as useractivity_http_referer
        , name as useractivity_event_name
        , event_type as useractivity_event_type
        , {{ extract_course_id_from_tracking_log() }} as courserun_readable_id
        --- extract common fields from context object
        , coalesce(
            {{ try_cast(json_query_string('context', "'$.user_id'"), 'integer') }}
            , {{ try_cast(json_query_string('event', "'$.user_id'"), 'integer') }}
        ) as openedx_user_id
        , {{ json_query_string('context', "'$.org_id'") }} as org_id
        , {{ json_query_string('context', "'$.path'") }} as useractivity_path
        --- due to log collector changes, values of time field come with different formats
        , {{ format_timestamp_as_iso8601(from_iso8601_timestamp_nanos(
            regexp_replace_with_backreferences('time', "'(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2}\.\d+)(.*?)'", "'$1T$2$3'")
        )) }} as useractivity_timestamp -- noqa
        , _dlt_load_id as useractivity_load_id
    from most_recent_source
)

select * from cleaned
