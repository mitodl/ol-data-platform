with micromasters_exam_grades as (
    select * from {{ ref('int__micromasters__dedp_proctored_exam_grades') }}
)

, mitxonline_exam_grades as (
    select * from {{ ref('int__mitxonline__proctored_exam_grades') }}
)

, micromasters_users as (
    select * from {{ ref('int__micromasters__users') }}
)

-- semester + passing_grade for MITxOnline proctored exam runs are now sourced from
-- dim_course_run (added in #2319) instead of being re-derived here. The pure MicroMasters
-- branch below (micromasters_exam_grades) still reads these fields directly from
-- int__micromasters__dedp_proctored_exam_grades because those exam runs are not represented
-- in dim_course_run until MicroMasters grades are added to tfact_grade (tracked in epic #2072).
-- Guard against dim_course_run SCD2 expiration gap: multiple is_current=true rows
-- for the same courserun_readable_id can fan out mitxonline_exam_grades rows.
-- Pick the latest, matching the established pattern in dim_product.
, mitxonline_courserun_metadata as (
    select courserun_readable_id, semester, passing_grade
    from (
        select
            courserun_readable_id
            , semester
            , passing_grade
            , row_number() over (
                partition by courserun_readable_id
                order by effective_date desc nulls last
            ) as _row_num
        from {{ ref('dim_course_run') }}
        where platform = 'mitxonline' and is_current
    )
    where _row_num = 1
)

select
    course_number
    , course_title
    , examrun_readable_id as examrun_courserun_readable_id
    , user_edxorg_username
    , user_mitxonline_username
    , user_full_name
    , user_micromasters_email
    , user_mitxonline_email
    , examrun_passing_grade as proctoredexamgrade_passing_grade
    , proctoredexamgrade_percentage_grade
    , proctoredexamgrade_created_on
    , examrun_semester as semester
from micromasters_exam_grades

union all

select
    mitxonline_exam_grades.course_number
    , mitxonline_exam_grades.course_title
    , mitxonline_exam_grades.courserun_readable_id as examrun_courserun_readable_id
    , mitxonline_exam_grades.user_edxorg_username
    , mitxonline_exam_grades.user_username as user_mitxonline_username
    , mitxonline_exam_grades.user_full_name
    , micromasters_users.user_email as user_micromasters_email
    , mitxonline_exam_grades.user_email as user_mitxonline_email
    , mitxonline_courserun_metadata.passing_grade as proctoredexamgrade_passing_grade
    , mitxonline_exam_grades.proctoredexamgrade_grade as proctoredexamgrade_percentage_grade
    , mitxonline_exam_grades.proctoredexamgrade_created_on
    , mitxonline_courserun_metadata.semester
from mitxonline_exam_grades
left join micromasters_users on mitxonline_exam_grades.user_username = micromasters_users.user_mitxonline_username
left join mitxonline_courserun_metadata
    on mitxonline_exam_grades.courserun_readable_id = mitxonline_courserun_metadata.courserun_readable_id
left join micromasters_exam_grades
    on
        mitxonline_exam_grades.courserun_readable_id = micromasters_exam_grades.examrun_readable_id
        and mitxonline_exam_grades.user_username = micromasters_exam_grades.user_mitxonline_username
where micromasters_exam_grades.user_mitxonline_username is null and micromasters_exam_grades.examrun_readable_id is null
