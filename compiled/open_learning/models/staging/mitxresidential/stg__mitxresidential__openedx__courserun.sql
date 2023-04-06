with source as (
    select * from dev.main_raw.raw__mitx__openedx__mysql__course_overviews_courseoverview
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
        ,
        to_iso8601(from_iso8601_timestamp("start"))
        as courserun_start_on
        ,
        to_iso8601(from_iso8601_timestamp("end"))
        as courserun_end_on
        ,
        to_iso8601(from_iso8601_timestamp(enrollment_start))
        as courserun_enrollment_start_on
        ,
        to_iso8601(from_iso8601_timestamp(enrollment_end))
        as courserun_enrollment_end_on
        ,
        to_iso8601(from_iso8601_timestamp(announcement))
        as courserun_announce_on
        ,
        to_iso8601(from_iso8601_timestamp(certificate_available_date))
        as courserun_certificate_available_on
        ,
        to_iso8601(from_iso8601_timestamp(created))
        as courserun_created_on
        ,
        to_iso8601(from_iso8601_timestamp(modified))
        as courserun_updated_on
    from source
)

select * from cleaned
