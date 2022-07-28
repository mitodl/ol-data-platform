-- MITx Online Course Run Enrollment Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','mitxonline__app__postgres__courses_courserunenrollment') }}
)

, cleaned as (
    select
        -- int, sequential ID tracking a single user enrollment
        id
        -- str, boolean describing whether the enrollment is active
        , active
        -- int, unique ID specifying a "run" of an MITx Online course
        , run_id
        -- int, unique ID for each user on the MITx Online platform
        , user_id
        -- timestamp, specifying when an enrollment was initially created
        , cast(created_on[1] as timestamp(6)) as created_on
        -- timestamp, specifying when an enrollment was most recently updated
        , cast(updated_on[1] as timestamp(6)) as updated_on
    from source
)

select * from cleaned
