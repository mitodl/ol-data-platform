-- MITx Online Course Run Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__courses_courserun') }}
)

, cleaned as (
    select
        id
        , live
        , title
        , end_date
        , start_date
        , course_id
        , courseware_id
        , courseware_url_path
        , created_on
        , updated_on
    from source
)

select * from cleaned
