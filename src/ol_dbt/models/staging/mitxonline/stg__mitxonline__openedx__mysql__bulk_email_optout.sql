-- MITx Online Openedx mySQL Bulk Email Optout

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__bulk_email_optout') }}
)

, cleaned as (
    select
        id as email_optout_id
        , user_id as openedx_user_id
        , course_id as courserun_readable_id
    from source
)

select * from cleaned
