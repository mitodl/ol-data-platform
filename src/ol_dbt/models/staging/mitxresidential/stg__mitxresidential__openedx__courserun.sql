with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__course_overviews_courseoverview') }}
)

, cleaned as (
    select
        id as courserun_readable_id
        , display_name as courserun_title
        , org as courserun_org
        , display_number_with_default as courserun_course_number
        , course_video_url as courserun_video_url
        , lowest_passing_grade as courserun_passing_grade
        , self_paced as courserun_is_self_paced
        , eligible_for_financial_aid as courserun_is_eligible_for_financial_aid
        , {{ cast_timestamp_to_iso8601('"start"') }} as courserun_start_on
        , {{ cast_timestamp_to_iso8601('"end"') }} as courserun_end_on
        , {{ cast_timestamp_to_iso8601('enrollment_start') }} as courserun_enrollment_start_on
        , {{ cast_timestamp_to_iso8601('enrollment_end') }} as courserun_enrollment_end_on
        , {{ cast_timestamp_to_iso8601('announcement') }} as courserun_announce_on
        , {{ cast_timestamp_to_iso8601('certificate_available_date') }} as courserun_certificate_available_on
        , {{ cast_timestamp_to_iso8601('created') }} as courserun_created_on
        , {{ cast_timestamp_to_iso8601('modified') }} as courserun_updated_on
    from source
)

select * from cleaned
