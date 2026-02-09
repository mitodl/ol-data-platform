-- Course Run Grades information from edx.org

with grades as (
    select
        user_id
        , courserun_readable_id
        , courserungrade_passing_grade
        , courserungrade_user_grade
        , courserungrade_is_passing
    from {{ ref('stg__edxorg__bigquery__mitx_person_course') }}
    where
        courserun_platform = '{{ var("edxorg") }}'
        and courserungrade_user_grade is not null
)

, user_info_combo as (
    select *
    from {{ ref('stg__edxorg__bigquery__mitx_user_info_combo') }}
    where
        courserun_platform = '{{ var("edxorg") }}'
        and courseruncertificate_grade is not null
)

, runs as (
    select * from {{ ref('int__edxorg__mitx_courseruns') }}
)

, users as (
    select * from {{ ref('int__edxorg__mitx_users') }}
)

, micromasters_users as (
    select * from {{ ref('__micromasters__users') }}
)

, edxorg_grades as (
    select
        coalesce(
            grades.courserun_readable_id,
            user_info_combo.courseruncertificate_courserun_readable_id
         ) as courserun_readable_id
        , coalesce(
            grades.user_id,
            user_info_combo.user_id
         ) as user_id
        , coalesce(
            grades.courserungrade_user_grade,
            user_info_combo.courseruncertificate_grade
        ) as courserungrade_user_grade
        , grades.courserungrade_passing_grade
        , grades.courserungrade_is_passing
    from grades
    full outer join user_info_combo on
        grades.user_id = user_info_combo.user_id
        and grades.courserun_readable_id = user_info_combo.courseruncertificate_courserun_readable_id
)

, final as (
    select
        edxorg_grades.courserun_readable_id
        , edxorg_grades.courserungrade_passing_grade
        , edxorg_grades.courserungrade_user_grade
        , edxorg_grades.courserungrade_is_passing
        , users.user_id
        , users.user_email
        , users.user_username
        , users.user_full_name
        , micromasters_users.user_mitxonline_username
        , runs.courserun_title
        , runs.course_number
        , runs.micromasters_program_id
    from edxorg_grades
    inner join users on edxorg_grades.user_id = users.user_id
    left join runs on edxorg_grades.courserun_readable_id = runs.courserun_readable_id
    left join micromasters_users on users.user_username = micromasters_users.user_edxorg_username
)

select * from final
