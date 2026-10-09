with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__courses_program_enrollment_modes') }}
)

{{ deduplicate_raw_table(raw_table='raw__mitxonline__app__postgres__courses_program_enrollment_modes', partition_columns='id') }}

, cleaned as (
    select
        id as programenrollmentmode_id
        , program_id
        , enrollmentmode_id
    from most_recent_source
)

select * from cleaned
