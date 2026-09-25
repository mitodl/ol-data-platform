with micromasters_exam_grades as (
    select * from {{ ref('int__micromasters__dedp_proctored_exam_grades') }}
)

, mitxonline_exam_grades as (
    select * from {{ ref('int__mitxonline__proctored_exam_grades') }}
)

, micromasters_users as (
    select * from {{ ref('int__micromasters__users') }}
)

-- MITxOnline semester + passing_grade are sourced from dim_course_run (#2319).
-- The MicroMasters branch below still reads them from int__micromasters__dedp_proctored_exam_grades
-- because those exam runs are absent from dim_course_run until MicroMasters grades reach tfact_grade (#2072).
--
-- dim_course_run can temporarily hold multiple is_current rows per course run during an SCD2
-- expiration gap, which would fan this join out. Take the newest, as dim_product does.
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
