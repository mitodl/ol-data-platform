-- MicroMasters program elective set Information

with source as (
    select * from dev.main_raw.raw__micromasters__app__postgres__courses_electivesset
)

, cleaned as (
    select
        id as electiveset_id
        , program_id
        , title as electiveset_title
        , required_number as electiveset_required_number
    from source
)

select * from cleaned
