-- MicroMasters course to electives set Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__micromasters__app__postgres__courses_electivecourse') }}
)

, cleaned as (
    select
        id as electivesettocourse_id
        , course_id
        , electives_set_id as electiveset_id
    from source
)

select * from cleaned
