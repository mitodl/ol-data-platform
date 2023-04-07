with course_grades_dedp_from_micromasters as (
    select *
    from {{ ref('__micromasters_course_grades_dedp_from_micromasters') }}
)

, course_grades_dedp_from_mitxonline as (
    select *
    from {{ ref('__micromasters_course_grades_dedp_from_mitxonline') }}
)

, course_grades_non_dedp_program as (
    select *
    from {{ ref('__micromasters_course_grades_non_dedp_from_edxorg') }}
)

-- DEDP course certificates come from MicroMasters and MITxOnline. We've migrated some learners data from
-- MicroMasters to MITxOnline around Oct 2022, but only for those users who have MITxOnline account.
-- To avoid data overlapping, we use the cut-off date 2022-10-01 and their linked MITxOnline account to dedup

, dedp_course_grades_combined as (
    select
        program_title
        , courserun_title
        , courserun_readable_id
        , course_number
        , user_edxorg_username
        , user_mitxonline_username
        , user_full_name
        , user_country
        , user_email
        , coursegrade_grade as grade
        , true as is_passing
        , coursegrade_created_on as created_on
    from course_grades_dedp_from_micromasters
    where coursegrade_created_on < '2022-10-01'

    union all

    select
        program_title
        , courserun_title
        , courserun_readable_id
        , course_number
        , user_edxorg_username
        , user_mitxonline_username
        , user_full_name
        , user_country
        , user_email
        , courserungrade_grade as grade
        , courserungrade_is_passing as is_passing
        , courserungrade_created_on as created_on
    from course_grades_dedp_from_mitxonline
    where courserungrade_created_on >= '2022-10-01'

)

, dedp_course_grades_sorted as (
    select
        program_title
        , courserun_title
        , courserun_readable_id
        , course_number
        , user_edxorg_username
        , user_mitxonline_username
        , user_full_name
        , user_country
        , user_email
        , grade
        , is_passing
        , case
            when
                user_mitxonline_username is not null then
                row_number() over (
                    partition by courserun_readable_id, user_mitxonline_username order by created_on desc
                )
            else 1
        end as row_num
    from dedp_course_grades_combined
)

, dedp_course_grades as (
    select *
    from dedp_course_grades_sorted
    where row_num = 1
)

, program_course_grades as (
    select
        program_title
        , courserun_title
        , courserun_readable_id
        , course_number
        , user_edxorg_username
        , user_mitxonline_username
        , user_full_name
        , user_country
        , user_email
        , grade
        , is_passing
    from dedp_course_grades

    union all

    select
        program_title
        , courserun_title
        , courserun_readable_id
        , course_number
        , user_edxorg_username
        , user_mitxonline_username
        , user_full_name
        , user_country
        , user_email
        , courserungrade_user_grade as grade
        , courserungrade_is_passing as is_passing
    from course_grades_non_dedp_program
)

select * from program_course_grades
