with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__courses_enrollmentmode') }}
)

{{ deduplicate_raw_table(raw_table='raw__mitxonline__app__postgres__courses_enrollmentmode', partition_columns='id') }}

, cleaned as (
    select
        id as enrollmentmode_id
        , mode_slug as enrollmentmode_slug
        , mode_display_name as enrollmentmode_display_name
        , requires_payment as enrollmentmode_requires_payment
    from most_recent_source
)

select * from cleaned
