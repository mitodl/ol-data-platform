with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__google_ads__s3__change_history_report') }}
)

select
    course as change_course
    , row_number as change_row_number
    -- The account's local time; the UI puts a narrow no-break space before AM/PM.
    , {{ date_parse("replace(date_time, chr(8239), ' ')", "'%b %d, %Y, %I:%i:%s %p'") }} as change_at
    , campaign as campaign_name
    , ad_group as ad_group_name
    , {{ google_ads_mask_emails('changes') }} as change_text
    , _source_file as change_source_file
from source
