-- MITx Online CourseRunEnrollment audit log (before/after change tracking)

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__courses_courserunenrollmentaudit') }}
)

, cleaned as (

    select
        id as enrollmentaudit_id
        , enrollment_id as courserunenrollment_id
        , acting_user_id
        , data_before as enrollmentaudit_data_before
        , data_after as enrollmentaudit_data_after
        , {{ cast_timestamp_to_iso8601('created_on') }} as enrollmentaudit_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as enrollmentaudit_updated_on
    from source
)

select * from cleaned
