with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__google_ads__s3__change_history_report') }}
)

select
    course as change_course
    , {{ adapter.quote('row_number') }} as change_row_number
    -- The UI puts a narrow no-break space before AM/PM.
    , {{ google_ads_local_to_utc(
        date_parse("replace(date_time, " ~ codepoint_char(8239) ~ ", ' ')", "'%b %d, %Y, %I:%i:%s %p'")
        , google_ads_default_time_zone()
    ) }}
    as change_at
    , {{ adapter.quote('user') }} as change_user
    , campaign as campaign_name
    , ad_group as ad_group_name
    , changes as change_text
    , _source_file as change_source_file
    , _dlt_load_id as change_dlt_load_id
from source
