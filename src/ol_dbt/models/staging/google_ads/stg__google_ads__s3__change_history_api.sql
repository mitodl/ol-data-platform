with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__google_ads__s3__change_history_api') }}
)

select
    course as change_course
    , line_number as change_line_number
    , {{ google_ads_local_to_utc(adapter.quote('timestamp'), google_ads_default_time_zone()) }} as change_at
    , campaign as campaign_name
    , ad_group as ad_group_name
    , header as change_header
    , details as change_details
    , _source_file as change_source_file
    , _dlt_load_id as change_dlt_load_id
from source
