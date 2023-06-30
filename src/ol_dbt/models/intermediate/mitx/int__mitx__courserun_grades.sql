--- MITx course grades combined from MITx Online and edX.org

{{ config(materialized='view') }}

with mitxonline_grades as (
    select *
    from {{ ref('int__mitxonline__courserun_grades') }}
    where courserun_platform = '{{ var("mitxonline") }}'
)

, mitxonline_dedp_courses as (
    select course_id from {{ ref('int__mitxonline__program_to_courses') }}
    where program_id = {{ var("dedp_mitxonline_program_id") }}
)

, edxorg_non_program_course_grades as (
    select * from {{ ref('int__edxorg__mitx_courserun_grades') }}
    where micromasters_program_id is null
)

, all_program_course_grades as (
    select * from {{ ref('int__micromasters__course_grades') }}
)

, mitxonline_non_dedp_course_grades as (
    select mitxonline_grades.*
    from mitxonline_grades
    left join mitxonline_dedp_courses on mitxonline_grades.course_id = mitxonline_dedp_courses.course_id
    left join all_program_course_grades
        on
            mitxonline_grades.courserun_readable_id = all_program_course_grades.courserun_readable_id
            and mitxonline_grades.user_username = all_program_course_grades.user_mitxonline_username
    where mitxonline_dedp_courses.course_id is null and all_program_course_grades.courserun_readable_id is null
)

, mitx_grades as (
    select
        '{{ var("mitxonline") }}' as platform
        , course_number
        , courserun_title
        , courserun_readable_id
        , courserungrade_grade
        , courserungrade_is_passing
        , user_email
        , user_full_name
        , user_edxorg_username
        , user_username as user_mitxonline_username
    from mitxonline_non_dedp_course_grades

    union all

    select
        '{{ var("edxorg") }}' as platform
        , course_number
        , courserun_title
        , courserun_readable_id
        , courserungrade_user_grade as courserungrade_grade
        , courserungrade_is_passing
        , user_email
        , user_full_name
        , user_username as user_edxorg_username
        , user_mitxonline_username
    from edxorg_non_program_course_grades

    union all

    select
        courserun_platform as platform
        , course_number
        , courserun_title
        , courserun_readable_id
        , grade as courserungrade_grade
        , is_passing as courserungrade_is_passing
        , user_email
        , user_full_name
        , user_edxorg_username
        , user_mitxonline_username
    from all_program_course_grades
)

select * from mitx_grades
