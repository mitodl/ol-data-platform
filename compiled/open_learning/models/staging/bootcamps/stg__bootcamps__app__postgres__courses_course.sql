-- Bootcamps Course Information

with source as (
    select * from dev.main_raw.raw__bootcamps__app__postgres__klasses_bootcamp
)

, cleaned as (
    select
        id as course_id
        , title as course_title
    from source
)

select * from cleaned
