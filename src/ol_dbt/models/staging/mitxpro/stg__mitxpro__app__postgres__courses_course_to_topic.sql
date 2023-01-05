-- MITxPro Course to Topic Information

with source as (
    select *
    from
        {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__courses_course_topics') }}
)

, cleaned as (
    select
        id as coursetotopic_id
        , course_id
        , coursetopic_id
    from source
)

select * from cleaned
