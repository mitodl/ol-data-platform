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
-- To avoid data overlapping, we deduplicate based on their social auth account linked on MicroMasters.
-- for old DEDP courses on edx.org, then we use certificates from MicroMasters
-- for new DEDP course on MITx Online, then we use certificates from MITx Online


, dedp_course_certificates_combined as (
    select
        program_title
        , micromasters_program_id
        , mitxonline_program_id
        , courserun_title
        , courserun_readable_id
        , courserun_platform
        , course_number
        , user_edxorg_username
        , user_mitxonline_username
        , user_full_name
        , user_country
        , user_email
        , coursecertificate_url as courseruncertificate_url
        , coursecertificate_created_on as courseruncertificate_created_on
    from course_certificates_dedp_from_micromasters
    where courserun_platform = '{{ var("edxorg") }}'

    union all

    select
        program_title
        , micromasters_program_id
        , mitxonline_program_id
        , courserun_title
        , courserun_readable_id
        , courserun_platform
        , course_number
        , user_edxorg_username
        , user_mitxonline_username
        , user_full_name
        , user_country
        , user_email
        , courseruncertificate_url
        , courseruncertificate_created_on
    from course_certificates_dedp_from_mitxonline
    where courserun_platform = '{{ var("mitxonline") }}'

)

, dedp_course_certificates_sorted as (
    select
        program_title
        , micromasters_program_id
        , mitxonline_program_id
        , courserun_title
        , courserun_readable_id
        , courserun_platform
        , course_number
        , user_edxorg_username
        , user_mitxonline_username
        , user_full_name
        , user_country
        , user_email
        , courseruncertificate_url
        , courseruncertificate_created_on
        , case
            when
                user_mitxonline_username is not null
                then
                    row_number()
                        over (
                            partition by courserun_readable_id, user_mitxonline_username, mitxonline_program_id
                            order by courseruncertificate_created_on desc
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
        , courseruncertificate_url
        , courseruncertificate_created_on
    from dedp_course_certificates

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
        , courseruncertificate_download_url as courseruncertificate_url
        , courseruncertificate_created_on
    from course_certificates_non_dedp_program
)

select * from program_course_certificates
