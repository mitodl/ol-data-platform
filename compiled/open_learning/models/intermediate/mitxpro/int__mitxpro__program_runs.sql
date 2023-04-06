-- Program Runs information for MITxPro

with program_runs as (
    select * from dev.main_staging.stg__mitxpro__app__postgres__courses_programrun
)

, programs as (
    select * from dev.main_staging.stg__mitxpro__app__postgres__courses_program
)

select
    program_runs.programrun_id
    , programs.program_id
    , programs.program_title
    , program_runs.programrun_tag
    , program_runs.programrun_start_on
    , program_runs.programrun_end_on
    , concat(programs.program_readable_id, '+', program_runs.programrun_tag) as programrun_readable_id
from program_runs
inner join programs on programs.program_id = program_runs.program_id
