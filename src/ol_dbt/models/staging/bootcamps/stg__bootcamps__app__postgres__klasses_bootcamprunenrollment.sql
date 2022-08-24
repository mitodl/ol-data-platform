-- Bootcamps Course Run Enrollment Information

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__bootcamps__app__postgres__klasses_bootcamprunenrollment') }}
)

, renamed as (
    select
        id
        , active
        , user_id
        , created_on
        , updated_on
        , change_status
        , bootcamp_run_id
        , novoed_sync_date
    from source
)

select * from renamed
