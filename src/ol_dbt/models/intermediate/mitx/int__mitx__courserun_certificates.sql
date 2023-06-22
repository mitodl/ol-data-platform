--- MITx course certificates combined from MITx Online and edX.org
--- The logic is to combine from:
---- Non-DEDP course certificate from MITx Online
---- MicroMasters program courses certificate (combined DEDP and other MM program courses)
---- course certificates from edx.org, excluding DEDP and other MM program course certificates

{{ config(materialized='view') }}

with mitxonline_certificates as (
    select *
    from {{ ref('int__mitxonline__courserun_certificates') }}
    where
        courserun_platform = '{{ var("mitxonline") }}'
        and courseruncertificate_is_revoked = false
)

, mitxonline_dedp_courses as (
    select course_id from {{ ref('int__mitxonline__program_to_courses') }}
    where program_id = {{ var("dedp_mitxonline_program_id") }}
)

, mitxonline_non_dedp_course_certificates as (
    select mitxonline_certificates.*
    from mitxonline_certificates
    left join mitxonline_dedp_courses on mitxonline_certificates.course_id = mitxonline_dedp_courses.course_id
    where mitxonline_dedp_courses.course_id is null
)

, edxorg_non_program_course_certificates as (
    select * from {{ ref('int__edxorg__mitx_courserun_certificates') }}
    where micromasters_program_id is null
)

, program_course_certificates as (
    select * from {{ ref('int__micromasters__course_certificates') }}
)

, mitx_certificates as (
    select
        '{{ var("mitxonline") }}' as platform
        , course_number
        , courserun_title
        , courserun_readable_id
        , courseruncertificate_url
        , courseruncertificate_created_on
        , user_username as user_mitxonline_username
        , user_edxorg_username
        , user_email
        , user_full_name
    from mitxonline_non_dedp_course_certificates

    union all

    select
        '{{ var("edxorg") }}' as platform
        , course_number
        , courserun_title
        , courserun_readable_id
        , courseruncertificate_download_url as courseruncertificate_url
        , courseruncertificate_created_on
        , user_mitxonline_username
        , user_username as user_edxorg_username
        , user_email
        , user_full_name
    from edxorg_non_program_course_certificates

    union all

    select
        courserun_platform as platform
        , course_number
        , courserun_title
        , courserun_readable_id
        , courseruncertificate_url
        , courseruncertificate_created_on
        , user_mitxonline_username
        , user_edxorg_username
        , user_email
        , user_full_name
    from program_course_certificates
)

select * from mitx_certificates
