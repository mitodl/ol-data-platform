with exam_unit_grades as (
    select * from {{ ref('int__mitxonline__courserun_subsection_grades') }}
    where
        lower(coursestructure_block_title) = 'proctored exam'
        and subsectiongrade_total_earned_graded_score > 0
)

, exam_courserun_grades as (
    select * from {{ ref('int__mitxonline__courserun_grades') }}
    where lower(courserun_title) like '%proctored exam'
)

, exam_courseruns_from_micromasters as (
    --- pulling exam courseruns from MicroMasters to associate a exam run with a course
    --- since all these MITxT exam runs and their associations to courses exist in MicroMasters.
    select
        examrun_readable_id
        , examrun_semester
        , examrun_passing_grade
        , course_id
    from {{ ref('stg__micromasters__app__postgres__exams_examrun') }}
)

, courses_from_micromasters as (
    select * from {{ ref('stg__micromasters__app__postgres__courses_course') }}
)

, courseruns_from_mitxonline as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_courserun') }}
)

, courses_from_mitxonline as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_course') }}
)

, users as (
    select * from {{ ref('int__mitxonline__users') }}
)

select
    courses_from_mitxonline.course_number
    , courses_from_mitxonline.course_title
    , exam_courseruns_from_micromasters.examrun_semester as semester
    , exam_courserun_grades.courserun_readable_id
    , users.user_id
    , users.openedx_user_id
    , users.user_edxorg_username
    , users.user_username
    , users.user_full_name
    , users.user_email
    , exam_courseruns_from_micromasters.examrun_passing_grade as proctoredexamgrade_passing_grade
    , exam_courserun_grades.courserungrade_grade as proctoredexamgrade_grade
    , exam_courserun_grades.courserungrade_created_on as proctoredexamgrade_created_on
from exam_courserun_grades
inner join users on exam_courserun_grades.user_id = users.user_id
left join exam_courseruns_from_micromasters
    on exam_courserun_grades.courserun_readable_id = exam_courseruns_from_micromasters.examrun_readable_id
left join courses_from_micromasters on exam_courseruns_from_micromasters.course_id = courses_from_micromasters.course_id
left join courses_from_mitxonline on courses_from_micromasters.course_number = courses_from_mitxonline.course_number

union all

select
    courses_from_mitxonline.course_number
    , courses_from_mitxonline.course_title
    , courseruns_from_mitxonline.courserun_tag as semester
    , exam_unit_grades.courserun_readable_id
    , users.user_id
    , users.openedx_user_id
    , users.user_edxorg_username
    , users.user_username
    , users.user_full_name
    , users.user_email
    , null as proctoredexamgrade_passing_grade
    , exam_unit_grades.subsectiongrade_total_earned_graded_score / exam_unit_grades.subsectiongrade_total_graded_score
        as proctoredexamgrade_grade
    , exam_unit_grades.subsectiongrade_created_on as proctoredexamgrade_created_on
from exam_unit_grades
inner join users on exam_unit_grades.openedx_user_id = users.openedx_user_id
inner join
    courseruns_from_mitxonline
    on exam_unit_grades.courserun_readable_id = courseruns_from_mitxonline.courserun_readable_id
inner join courses_from_mitxonline on courseruns_from_mitxonline.course_id = courses_from_mitxonline.course_id
left join exam_courserun_grades
    on
        users.user_username = exam_courserun_grades.user_username
        and exam_unit_grades.courserun_readable_id = exam_courserun_grades.courserun_readable_id
where exam_courserun_grades.courserungrade_id is null
