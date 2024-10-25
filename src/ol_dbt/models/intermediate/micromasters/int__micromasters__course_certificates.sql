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

, mitx_users as (
    select * from {{ ref('int__mitx__users') }}
)

-- DEDP course certificates come from MicroMasters and MITxOnline. We've migrated some learners data from
-- MicroMasters to MITxOnline around Oct 2022, but only for those users who have MITxOnline account.
-- To avoid data overlapping, we deduplicate based on their social auth account linked on MicroMasters.
-- for old DEDP courses on edx.org, then we use certificates from MicroMasters
-- for new DEDP course on MITx Online, then we use certificates from MITx Online


, dedp_course_certificates_combined as (
    select distinct
        course_certificates_dedp_from_micromasters.program_title
        , course_certificates_dedp_from_micromasters.micromasters_program_id
        , course_certificates_dedp_from_micromasters.mitxonline_program_id
        , course_certificates_dedp_from_micromasters.courserun_title
        , course_certificates_dedp_from_micromasters.courserun_readable_id
        , course_certificates_dedp_from_micromasters.courserun_platform
        , course_certificates_dedp_from_micromasters.course_number
        , mitx_users.user_edxorg_username
        , mitx_users.user_mitxonline_username
        , mitx_users.user_full_name
        , mitx_users.user_address_country as user_country
        , mitx_users.user_micromasters_email as user_email
        , course_certificates_dedp_from_mitxonline.courseruncertificate_uuid
        , course_certificates_dedp_from_mitxonline.courseruncertificate_url
        , course_certificates_dedp_from_mitxonline.courseruncertificate_created_on
    from course_certificates_dedp_from_micromasters
    left join mitx_users
        on course_certificates_dedp_from_micromasters.user_micromasters_id = mitx_users.user_micromasters_id
    inner join course_certificates_dedp_from_mitxonline
        on (
            mitx_users.user_mitxonline_id = course_certificates_dedp_from_mitxonline.user_mitxonline_id
            or mitx_users.user_micromasters_email = course_certificates_dedp_from_mitxonline.user_mitxonline_email
        )
     and course_certificates_dedp_from_micromasters.courserun_readable_id = course_certificates_dedp_from_mitxonline.courserun_readable_id
    where course_certificates_dedp_from_micromasters.courserun_platform = '{{ var("edxorg") }}'

    union all

    select
        course_certificates_dedp_from_mitxonline.program_title
        , course_certificates_dedp_from_mitxonline.micromasters_program_id
        , course_certificates_dedp_from_mitxonline.mitxonline_program_id
        , course_certificates_dedp_from_mitxonline.courserun_title
        , course_certificates_dedp_from_mitxonline.courserun_readable_id
        , course_certificates_dedp_from_mitxonline.courserun_platform
        , course_certificates_dedp_from_mitxonline.course_number
        , mitx_users.user_edxorg_username
        , mitx_users.user_mitxonline_username
        , mitx_users.user_full_name
        , mitx_users.user_address_country as user_country
        , mitx_users.user_mitxonline_email as user_email
        , course_certificates_dedp_from_mitxonline.courseruncertificate_uuid
        , course_certificates_dedp_from_mitxonline.courseruncertificate_url
        , course_certificates_dedp_from_mitxonline.courseruncertificate_created_on
    from course_certificates_dedp_from_mitxonline
    left join mitx_users
        on course_certificates_dedp_from_mitxonline.user_mitxonline_id = mitx_users.user_mitxonline_id
    where course_certificates_dedp_from_mitxonline.courserun_platform = '{{ var("mitxonline") }}'

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
        , courseruncertificate_uuid
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

, non_dedp_course_certificates as (
    select
        course_certificates_non_dedp_program.program_title
        , course_certificates_non_dedp_program.mitxonline_program_id
        , course_certificates_non_dedp_program.micromasters_program_id
        , course_certificates_non_dedp_program.courserun_title
        , course_certificates_non_dedp_program.courserun_readable_id
        , course_certificates_non_dedp_program.courserun_platform
        , course_certificates_non_dedp_program.course_number
        , mitx_users.user_edxorg_username
        , mitx_users.user_mitxonline_username
        , mitx_users.user_full_name
        , mitx_users.user_address_country as user_country
        , course_certificates_non_dedp_program.courseruncertificate_download_uuid as courseruncertificate_uuid
        , course_certificates_non_dedp_program.courseruncertificate_download_url as courseruncertificate_url
        , course_certificates_non_dedp_program.courseruncertificate_created_on
        , coalesce(mitx_users.user_edxorg_email, mitx_users.user_micromasters_email) as user_email
    from course_certificates_non_dedp_program
    left join mitx_users
        on course_certificates_non_dedp_program.user_edxorg_id = mitx_users.user_edxorg_id
)


, course_certificates as (
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
        , courseruncertificate_uuid
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
        , courseruncertificate_uuid
        , courseruncertificate_url
        , courseruncertificate_created_on
    from non_dedp_course_certificates
)

select *
from course_certificates
