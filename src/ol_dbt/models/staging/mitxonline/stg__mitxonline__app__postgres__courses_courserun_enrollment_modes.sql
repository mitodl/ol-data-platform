with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__courses_courserun_enrollment_modes') }}
)

{{ deduplicate_raw_table(raw_table='raw__mitxonline__app__postgres__courses_courserun_enrollment_modes', partition_columns='id') }}

, cleaned as (
    select
        id as courserunenrollmentmode_id
        , courserun_id
        , enrollmentmode_id
    from most_recent_source
)

select * from cleaned
