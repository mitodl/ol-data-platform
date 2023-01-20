-- Bootcamps Course Run Enrollment Information

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__bootcamps__app__postgres__klasses_bootcamprunenrollment') }}
)

, renamed as (
    select
        id as courserunenrollment_id
        , active as courserunenrollment_is_active
        , user_id
        , bootcamp_run_id as courserun_id
        , change_status as courserunenrollment_enrollment_status
        , user_certificate_is_blocked as courserunenrollment_is_certificate_blocked
        , {{ cast_timestamp_to_iso8601('created_on') }} as courserunenrollment_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as courserunenrollment_updated_on
        , {{ cast_timestamp_to_iso8601('novoed_sync_date') }} as courserunenrollment_novoed_sync_on
    from source
)

select * from renamed
