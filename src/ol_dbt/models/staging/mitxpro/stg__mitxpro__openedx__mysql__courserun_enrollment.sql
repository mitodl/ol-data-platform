with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__openedx__mysql__student_courseenrollment') }}
)

, cleaned as (
    select
        course_id as courserun_readable_id
        , user_id as openedx_user_id
        , is_active as courserunenrollment_is_active
        , mode as courserunenrollment_enrollment_mode
        , to_iso8601(created) as courserunenrollment_created_on
    from source
)

select * from cleaned
