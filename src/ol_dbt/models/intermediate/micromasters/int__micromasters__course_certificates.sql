with course_certificates_dedp_from_micromasters as (
    select *
    from {{ ref('__micromasters_course_certificates_dedp_from_micromasters') }}
)

, course_certificates_dedp_from_mitxonline as (
    select *
    from {{ ref('__micromasters_course_certificates_dedp_from_mitxonline') }}
)

, course_certificates_non_dedp_program as (
    select *
    from {{ ref('__micromasters_course_certificates_non_dedp_from_edxorg') }}
)

-- DEDP course certificates come from MicroMasters and MITxOnline. We've migrated some learners data from
-- MicroMasters to MITxOnline around Oct 2022, but only for those users who have MITxOnline account.
-- To avoid data overlapping, we use the cut-off date 2022-10-01 to reduce duplication, but there are still
-- a small number dups so need to apply additional logic to dedup based on their linked MITxOnline account


, dedp_course_certificates_combined as (
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
        , coursecertificate_created_on as created_on
    from course_certificates_dedp_from_micromasters
    where coursecertificate_created_on < '2022-10-01'

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
        , courseruncertificate_created_on as created_on
    from course_certificates_dedp_from_mitxonline
    where courseruncertificate_created_on >= '2022-10-01'

)

, dedp_course_certificates_sorted as (
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
        , case
            when
                user_mitxonline_username is not null
                then
                    row_number()
                        over (
                            partition by courserun_readable_id, user_mitxonline_username order by created_on desc
                        )
            else 1
        end as row_num
    from dedp_course_certificates_combined
)


, dedp_course_certificates as (
    select *
    from dedp_course_certificates_sorted
    where row_num = 1
)


, program_course_certificates as (
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
    from dedp_course_certificates

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
    from course_certificates_non_dedp_program
)

select * from program_course_certificates
