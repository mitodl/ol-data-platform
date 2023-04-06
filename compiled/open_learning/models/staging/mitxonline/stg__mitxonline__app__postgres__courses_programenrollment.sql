-- MITx Online Program Enrollment Information

with source as (
    select * from dev.main_raw.raw__mitxonline__app__postgres__courses_programenrollment
)

, cleaned as (
    select
        id as programenrollment_id
        , change_status as programenrollment_enrollment_status
        , active as programenrollment_is_active
        , program_id
        , user_id
        , enrollment_mode as programenrollment_enrollment_mode
        ,
        to_iso8601(from_iso8601_timestamp(created_on))
        as programenrollment_created_on
        ,
        to_iso8601(from_iso8601_timestamp(updated_on))
        as programenrollment_updated_on
    from source
)

select * from cleaned
