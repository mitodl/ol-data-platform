-- MicroMasters Program Information

with programs as (
    select * from dev.main_staging.stg__micromasters__app__postgres__courses_program
)

select
    program_id
    , program_title
    , program_description
    , program_is_live
    , program_num_required_courses
from programs
