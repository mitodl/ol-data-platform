-- Every course run that int__mitxonline__proctored_exam_grades reports with a semester must
-- carry that semester on its current dim_course_run row. Catches the dimension's hand-rolled
-- copy of the upstream gate drifting out of step with the upstream itself.
--
-- It does not catch a renamed or removed 'proctored exam' block: both sides read that same
-- predicate, so the run leaves both at once and there is nothing left to compare. That drift
-- drops grade rows from the mart entirely and has to be caught upstream, on the intermediate
-- model, keyed on block ids that have ever carried the title.
--
-- error_if is set here because the project default is `>10` (dbt_project.yml) and this returns
-- one row per affected course run — inheriting it would let a ten-run regression pass as a warn.
{{ config(error_if = '!= 0', warn_if = '!= 0') }}

-- LEFT join on purpose: a run with no current dimension row breaks the mart the same way a
-- NULL semester does; an inner join would drop exactly those rows.
select
    exam_grades.courserun_readable_id
    , count(*) as proctored_exam_grade_rows
from {{ ref('int__mitxonline__proctored_exam_grades') }} as exam_grades
left join {{ ref('dim_course_run') }} as course_run
    on
        exam_grades.courserun_readable_id = course_run.courserun_readable_id
        and course_run.platform = 'mitxonline'
        and course_run.is_current
where
    course_run.semester is null
    and exam_grades.semester is not null
group by exam_grades.courserun_readable_id
