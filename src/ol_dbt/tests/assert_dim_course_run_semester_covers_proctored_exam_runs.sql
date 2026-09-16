-- Guards the boundary between dim_course_run's `semester` and the derivation it replaced in
-- int__mitxonline__proctored_exam_grades: every course run that the upstream reports with a
-- semester must carry that semester on its current dimension row.
--
-- WHAT THIS CATCHES: divergence between the dimension's gate and the upstream derivation.
-- dim_course_run reproduces two upstream paths by hand — the MicroMasters exams_examrun
-- match, and the embedded-exam-unit fallback keyed on a course structure block titled
-- 'proctored exam', deduped to the newest snapshot per block. Any edit that moves one of
-- those out of step with the upstream (a changed dedup, a changed predicate, a dropped join)
-- shows up here as a populated upstream semester against a null dimension semester. A run
-- with no current dimension row at all is caught too, via the left join.
--
-- WHAT THIS DOES NOT CATCH, and why: source drift in course structure. If a 'proctored exam'
-- block is renamed or removed upstream, the dimension's gate and the upstream's own
-- exam_unit_grades CTE both read that same predicate, so the rows leave BOTH sides at once.
-- int__mitxonline__proctored_exam_grades simply stops reporting the run, this query has
-- nothing left to select, and it passes. Measured on production 2026-09-15: 72 of the 79
-- course runs with a semester depend on that shared predicate; only the 7 with a
-- MicroMasters exams_examrun record are independent of course structure.
--
-- Drift of that kind is a bigger failure than a null semester — it removes the grade rows
-- from marts__micromasters_dedp_exam_grades entirely — and it has to be detected upstream,
-- against a signal that does not itself depend on the current block title (graded attempts
-- on block ids that have historically carried it). That belongs on the intermediate model,
-- not here. Do not read this test as covering it.
--
-- error_if is overridden because the project sets `+error_if: ">10"` (dbt_project.yml), and
-- this query returns one row per affected course run. Inheriting that default would
-- downgrade a regression touching up to ten runs to a warning, which is the exact silent
-- failure the test exists to prevent.
{{ config(error_if = '!= 0', warn_if = '!= 0') }}

-- The join is deliberately a LEFT join: a proctored-exam run with no current dim_course_run
-- row at all does the same downstream damage as one with a NULL semester, because the mart
-- left-joins the dimension. An inner join would drop exactly those rows and pass.
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
