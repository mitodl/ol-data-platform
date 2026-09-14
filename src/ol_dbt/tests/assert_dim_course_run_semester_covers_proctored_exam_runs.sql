-- dim_course_run populates `semester` for MITxOnline runs from the MicroMasters exam-run
-- record, falling back to the run's own courserun_tag ONLY for runs carrying an embedded
-- proctored-exam unit. That gate is a live lookup against course structure
-- (coursestructure_block_title = 'proctored exam'), not a static property of the run.
--
-- So if course structure stops reporting that block for a run that genuinely has proctored
-- exam grades, `semester` silently becomes NULL and marts__micromasters_dedp_exam_grades
-- loses values with no error anywhere — exactly the 8,137-null regression seen on #2403
-- before the fallback existed. This test makes that failure loud instead.
--
-- Scoped to runs that already have a semester upstream: a run whose own source semester is
-- NULL is not evidence of a broken gate, and would make this test fire on pre-existing gaps
-- rather than on drift.
select
    exam_grades.courserun_readable_id
    , count(*) as proctored_exam_grade_rows
from {{ ref('int__mitxonline__proctored_exam_grades') }} as exam_grades
inner join {{ ref('dim_course_run') }} as course_run
    on
        exam_grades.courserun_readable_id = course_run.courserun_readable_id
        and course_run.platform = 'mitxonline'
        and course_run.is_current
where
    course_run.semester is null
    and exam_grades.semester is not null
group by exam_grades.courserun_readable_id
