-- MITxPro Course to Topic Information

with source as (
    select * from dev.main_raw.raw__xpro__app__postgres__courses_course_topics
)

, cleaned as (
    select
        id as coursetotopic_id
        , course_id
        , coursetopic_id
    from source
)

select * from cleaned
