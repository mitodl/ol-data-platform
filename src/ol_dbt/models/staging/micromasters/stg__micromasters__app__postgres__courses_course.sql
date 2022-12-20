-- MicroMasters Course metadata Information
-- It should be primarily used to link edx.org courses to programs in MicroMasters

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__micromasters__app__postgres__courses_course') }}
)

, cleaned as (
    select
        id as course_id
        , title as course_title
        , edx_key as course_edx_key
        , course_number
        , program_id
        , prerequisites as course_prerequisites
        , description as course_description
        , contact_email as course_contact_email
        , should_display_progress as course_should_display_progress
        , position_in_program as course_position_in_program
    from source
)

select * from cleaned
