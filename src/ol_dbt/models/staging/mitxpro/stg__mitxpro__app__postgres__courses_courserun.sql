-- MITxPro Course Run Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__courses_courserun') }}
)

, cleaned as (
    select
        id as courserun_id
        , course_id
        , live as courserun_is_live
        , title as courserun_title
        , courseware_id as courserun_readable_id
        , courseware_url_path as courserun_url
        , run_tag as courserun_tag
        , to_iso8601(from_iso8601_timestamp(start_date)) as courserun_start_on
        , to_iso8601(from_iso8601_timestamp(end_date)) as courserun_end_on
        , to_iso8601(from_iso8601_timestamp(enrollment_start)) as courserun_enrollment_start_on
        , to_iso8601(from_iso8601_timestamp(enrollment_end)) as courserun_enrollment_end_on
        , to_iso8601(from_iso8601_timestamp(expiration_date)) as courserun_expired_on
        , to_iso8601(from_iso8601_timestamp(created_on)) as courserun_created_on
        , to_iso8601(from_iso8601_timestamp(updated_on)) as courserun_updated_on
    from source
)

select * from cleaned
