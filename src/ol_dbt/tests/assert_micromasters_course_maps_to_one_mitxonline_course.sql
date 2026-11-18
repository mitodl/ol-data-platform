-- QA (RC) MITx Online has the same course number under several orgs (MITx, MITxT,
-- REPLICA) for 7 MicroMasters courses (2026-11-18); production has none.
{{ config(error_if='>0', severity=('warn' if raw_data_lake() == 'qa' else 'error')) }}

-- bridge_program_course keys a MicroMasters course to the MITx Online course with the
-- course number in its edx_key. A second MITx Online course with that number would add a
-- second course to the program without failing the bridge's uniqueness test.
select
    micromasters_courses.course_id
    , micromasters_courses.course_edx_key
    , count(*) as mitxonline_course_count
from {{ ref('stg__micromasters__app__postgres__courses_course') }} as micromasters_courses
inner join {{ ref('int__mitxonline__courses') }} as mitxonline_courses
    on {{ dbt.split_part('micromasters_courses.course_edx_key', "'/'", 2) }} = mitxonline_courses.course_number
group by micromasters_courses.course_id, micromasters_courses.course_edx_key
having count(*) > 1
