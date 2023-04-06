-- MicroMasters Program Information

with source as (
    select * from dev.main_raw.raw__micromasters__app__postgres__courses_program
)

, cleaned as (
    select
        id as program_id
        , live as program_is_live
        , title as program_title
        , description as program_description
        , price as program_price
        , num_required_courses as program_num_required_courses
        , ga_tracking_id as program_ga_tracking_id
        , financial_aid_availability as program_is_financial_aid_available
        ,
        to_iso8601(from_iso8601_timestamp(created_on))
        as program_created_on
        ,
        to_iso8601(from_iso8601_timestamp(updated_on))
        as program_updated_on
    from source
)

select * from cleaned
