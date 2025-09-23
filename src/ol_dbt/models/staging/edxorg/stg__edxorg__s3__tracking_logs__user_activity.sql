{{
    config(
        materialized="incremental",
        unique_key=[
            "user_username",
            "useractivity_context_object",
            "useractivity_event_source",
            "useractivity_event_type",
            "useractivity_event_object",
            "useractivity_timestamp",
        ],
        incremental_strategy="delete+insert",
        views_enabled=false,
    )
}}

with
    source as (
        select *
        from {{ source("ol_warehouse_raw_data", "raw__edxorg__s3__tracking_logs") }}
        where
            username != '' and json_query(context, 'lax $.user_id' omit quotes) is not null

            {% if is_incremental() %}
                and "time" > (select max(this.useractivity_timestamp) from {{ this }} as this)
            {% endif %}
    )

    {{
        deduplicate_raw_table(
            order_by='_airbyte_extracted_at desc, _ab_source_file_last_modified desc, "time"',
            partition_columns='username, context, event_source, event_type, event, "time"',
        )
    }},
    cleaned as (
        select
            username as user_username,
            context as useractivity_context_object,
            event as useractivity_event_object,
            event_source as useractivity_event_source,
            page as useractivity_page_url,
            session as useractivity_session_id,
            ip as useractivity_ip,
            host as useractivity_http_host,
            agent as useractivity_http_user_agent,
            accept_language as useractivity_http_accept_language,
            referer as useractivity_http_referer,
            name as useractivity_event_name,
            event_type as useractivity_event_type,
            {{ extract_course_id_from_tracking_log(course_id_has_old_format=true) }} as courserun_readable_id,
            cast(json_query(context, 'lax $.user_id' omit quotes) as integer) as user_id,
            json_query(context, 'lax $.org_id' omit quotes) as org_id,
            json_query(context, 'lax $.path' omit quotes) as useractivity_path,
            -- - use regex here to preserve the nanoseconds as date_parse truncates the fraction of second to
            -- milliseconds
            to_iso8601(
                from_iso8601_timestamp_nanos(
                    regexp_replace("time", '(\d{4}-\d{2}-\d{2})[ ](\d{2}:\d{2}:\d{2}\.\d+)(.*?)', '$1T$2$3')
                )
            ) as useractivity_timestamp
        from most_recent_source
    )

select *
from cleaned
