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

, mitx_users as (
    select * from {{ ref('int__mitx__users') }}
)

-- DEDP course grades come from MicroMasters and MITxOnline. We've migrated some learners data from
-- MicroMasters to MITxOnline around Oct 2022, but only for those users who have MITxOnline account.
-- To avoid data overlapping, we deduplicate based on their social auth account linked on MicroMasters.
-- for old DEDP courses on edx.org, then we use the grade from MicroMasters
-- for new DEDP course on MITx Online, then we use the grade from MITx Online

, dedp_course_grades_combined as (
    select
        course_grades_dedp_from_micromasters.program_title
        , course_grades_dedp_from_micromasters.mitxonline_program_id
        , course_grades_dedp_from_micromasters.micromasters_program_id
        , course_grades_dedp_from_micromasters.courserun_title
        , course_grades_dedp_from_micromasters.courserun_readable_id
        , course_grades_dedp_from_micromasters.courserun_platform
        , course_grades_dedp_from_micromasters.course_number
        , mitx_users.user_edxorg_username
        , mitx_users.user_mitxonline_username
        , mitx_users.user_full_name
        , mitx_users.user_address_country as user_country
        , mitx_users.user_micromasters_email as user_email
        , course_grades_dedp_from_micromasters.coursegrade_grade as grade
        , true as is_passing
        , course_grades_dedp_from_micromasters.coursegrade_created_on as created_on
    from course_grades_dedp_from_micromasters
    left join mitx_users
        on course_grades_dedp_from_micromasters.user_micromasters_id = mitx_users.user_micromasters_id
    where course_grades_dedp_from_micromasters.courserun_platform = '{{ var("edxorg") }}'

    union all

    select
        course_grades_dedp_from_mitxonline.program_title
        , course_grades_dedp_from_mitxonline.mitxonline_program_id
        , course_grades_dedp_from_mitxonline.micromasters_program_id
        , course_grades_dedp_from_mitxonline.courserun_title
        , course_grades_dedp_from_mitxonline.courserun_readable_id
        , course_grades_dedp_from_mitxonline.courserun_platform
        , course_grades_dedp_from_mitxonline.course_number
        , mitx_users.user_edxorg_username
        , mitx_users.user_mitxonline_username
        , mitx_users.user_full_name
        , mitx_users.user_address_country as user_country
        , mitx_users.user_mitxonline_email as user_email
        , course_grades_dedp_from_mitxonline.courserungrade_grade as grade
        , course_grades_dedp_from_mitxonline.courserungrade_is_passing as is_passing
        , course_grades_dedp_from_mitxonline.courserungrade_created_on as created_on
    from course_grades_dedp_from_mitxonline
    left join mitx_users
        on course_grades_dedp_from_mitxonline.user_mitxonline_username = mitx_users.user_mitxonline_username
    where course_grades_dedp_from_mitxonline.courserun_platform = '{{ var("mitxonline") }}'

)

, dedp_course_grades_sorted as (
    select
        program_title
        , mitxonline_program_id
        , micromasters_program_id
        , courserun_title
        , courserun_readable_id
        , courserun_platform
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
                user_mitxonline_username is not null
                then
                    row_number()
                        over (
                            partition by courserun_readable_id, user_mitxonline_username, mitxonline_program_id
                            order by created_on desc
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

, non_dedp_course_grades as (
    select
        course_grades_non_dedp_program.program_title
        , course_grades_non_dedp_program.mitxonline_program_id
        , course_grades_non_dedp_program.micromasters_program_id
        , course_grades_non_dedp_program.courserun_title
        , course_grades_non_dedp_program.courserun_readable_id
        , course_grades_non_dedp_program.courserun_platform
        , course_grades_non_dedp_program.course_number
        , mitx_users.user_edxorg_username
        , mitx_users.user_mitxonline_username
        , mitx_users.user_full_name
        , mitx_users.user_address_country as user_country
        , course_grades_non_dedp_program.courserungrade_user_grade as grade
        , course_grades_non_dedp_program.courserungrade_is_passing as is_passing
        , coalesce(mitx_users.user_edxorg_email, mitx_users.user_micromasters_email) as user_email
    from course_grades_non_dedp_program
    left join mitx_users
        on course_grades_non_dedp_program.user_edxorg_username = mitx_users.user_edxorg_username
)

, program_course_grades as (
    select
        program_title
        , mitxonline_program_id
        , micromasters_program_id
        , courserun_title
        , courserun_readable_id
        , courserun_platform
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
        , mitxonline_program_id
        , micromasters_program_id
        , courserun_title
        , courserun_readable_id
        , courserun_platform
        , course_number
        , user_edxorg_username
        , user_mitxonline_username
        , user_full_name
        , user_country
        , user_email
        , grade
        , is_passing
    from non_dedp_course_grades
)

select * from program_course_grades
