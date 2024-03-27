with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__micromasters__app__postgres__dashboard_programenrollment') }}
)

, cleaned as (
    select
        id as programenrollment_id
        , user_id
        , program_id
    from source
)

select * from cleaned
