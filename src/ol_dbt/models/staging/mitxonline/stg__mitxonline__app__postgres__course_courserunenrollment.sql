-- MITx Online Course Run Enrollment Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__courses_courserunenrollment') }}
)

, cleaned as (
    select
        id
        , active
        , run_id
        , user_id
        , created_on
        , updated_on
    from source
)

select * from cleaned
