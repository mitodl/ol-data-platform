-- xPro user activities from tracking logs
-- Raw table has duplicate rows introduced by our loading process - Airbyte incremental + append on
-- _ab_source_file_last_modified, thus need to dedupe in staging
{{ config(materialized='incremental', views_enabled=false, ) }}

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__openedx__tracking_logs') }}
    -- ignore blank username events since these don't supply user identifiers
    -- and those where event object has 'exception' field in it due to server errors
    -- e.g.{"exception":"<type 'exceptions.UnicodeEncodeError'>","event-type":"exception"}
    where
        username != ''
        and json_query(context, 'lax $.user_id' omit quotes) is not null
        and json_query(event, 'lax $.exception' omit quotes) is null
)

, source_sorted as (
    select
        *
        , row_number() over (
            partition by username, context, event_type, "time"  -- noqa
            order by _airbyte_emitted_at desc, _ab_source_file_last_modified desc, vector_timestamp desc
        ) as row_num
    from source
)

, dedup_source as (
    select *
    from source_sorted
    where row_num = 1
)

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
        , json_query(context, 'lax $.user_id' omit quotes) as openedx_user_id
        , json_query(context, 'lax $.org_id' omit quotes) as org_id
        , json_query(context, 'lax $.path' omit quotes) as useractivity_path
        --- due to log collector changes, values of time field come with different formats
        , to_iso8601(from_iso8601_timestamp_nanos(
            regexp_replace(time, '(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2}\.\d+)(.*?)', '$1T$2$3') -- noqa
        )) as useractivity_timestamp
    from dedup_source
)

select * from cleaned

{% if is_incremental() %}

    where useractivity_timestamp > (select max(useractivity_timestamp) from {{ this }})

{% endif %}
