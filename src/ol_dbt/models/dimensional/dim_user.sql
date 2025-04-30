with mitxonline_users as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__users_user') }}
)

, mitxonline_legaladdress as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__users_legaladdress') }}
)

, mitxonline_profile as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__users_userprofile') }}
)

, mitxonline_openedx_users as (
    select * from {{ ref('stg__mitxonline__openedx__mysql__auth_user') }}
)

, mitxonline_user_view as (
    select
        mitxonline_users.user_username
        , mitxonline_users.user_email
        , mitxonline_users.user_full_name
        , mitxonline_legaladdress.user_address_country
        , mitxonline_profile.user_highest_education
        , mitxonline_profile.user_gender
        , mitxonline_profile.user_birth_year
        , mitxonline_profile.user_company
        , mitxonline_profile.user_job_title
        , mitxonline_profile.user_industry
        , mitxonline_users.user_is_active
        , 'mitxonline' as platform
        , mitxonline_users.user_id
        , mitxonline_users.user_joined_on
    from mitxonline_users
    left join mitxonline_legaladdress on mitxonline_users.user_id = mitxonline_legaladdress.user_id
    left join mitxonline_profile on mitxonline_users.user_id = mitxonline_profile.user_id
)

-- MITx Pro Users
, mitxpro_users as (
    select * from {{ ref('stg__mitxpro__app__postgres__users_user') }}
)

, mitxpro_legaladdress as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__users_legaladdress') }}
)

, mitxpro_profile as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__users_profile') }}
)

, mitxpro_openedx_users as (
    select * from {{ ref('stg__mitxpro__openedx__mysql__auth_user') }}
)

, mitxpro_user_view as (
    select
        mitxpro_users.user_username
        , mitxpro_users.user_email
        , mitxpro_users.user_full_name
        , mitxpro_legaladdress.user_address_country
        , mitxpro_profile.user_highest_education
        , mitxpro_profile.user_gender
        , mitxpro_profile.user_birth_year
        , mitxpro_profile.user_company
        , mitxpro_profile.user_job_title
        , mitxpro_profile.user_industry
        , mitxpro_users.user_is_active
        , 'mitxpro' as platform
        , mitxpro_users.user_id
        , mitxpro_users.user_joined_on
    from mitxpro_users
    left join mitxpro_legaladdress on mitxpro_users.user_id = mitxpro_legaladdress.user_id
    left join mitxpro_profile on mitxpro_users.user_id = mitxpro_profile.user_id
)

-- Residential Users
, mitxresidential_openedx_users as (
    select * from {{ ref('stg__mitxresidential__openedx__auth_user') }}
)

, mitxresidential_profile as (
    select * from {{ ref('stg__mitxresidential__openedx__auth_userprofile') }}
)

, mitxresidential_user_view as (
    select
        mitxresidential_openedx_users.user_username
        , mitxresidential_openedx_users.user_email
        , mitxresidential_openedx_users.user_full_name
        , mitxresidential_profile.user_address_country
        , mitxresidential_profile.user_highest_education
        , mitxresidential_profile.user_gender
        , mitxresidential_profile.user_birth_year
        , mitxresidential_openedx_users.user_is_active
        , 'residential' as platform
        , mitxresidential_openedx_users.user_id
        , mitxresidential_openedx_users.user_joined_on
    from mitxresidential_openedx_users
    left join mitxresidential_profile on mitxresidential_openedx_users.user_id = mitxresidential_profile.user_id
)

-- edXorg Users
, edxorg_bigquery_user_info as (
    select * from {{ ref('stg__edxorg__bigquery__mitx_user_info_combo') }}
)

, edxorg_bigquery_person_course as (
    select * from {{ ref('stg__edxorg__bigquery__mitx_person_course') }}
)

, edxorg_bigquery_s3_user as (
    select * from {{ ref('stg__edxorg__s3__user') }}
)

, edxorg_user_view as (
    select
        user_info.user_full_name
        , coalesce(user_info.user_username, person_course.user_username, s3_user.user_username) as user_username
        , coalesce(user_info.user_email, s3_user.user_email) as user_email
        , coalesce(user_info.user_country, person_course.user_profile_country) as user_address_country
        , coalesce(user_info.user_highest_education, person_course.user_highest_education) as user_highest_education
        , coalesce(user_info.user_gender, person_course.user_gender) as user_gender
        , coalesce(user_info.user_birth_year, person_course.user_birth_year) as user_birth_year
        , coalesce(
            coalesce(user_info.courserunenrollment_is_active = 1, false)
            , person_course.courserunenrollment_is_active
            , s3_user.user_is_active
        ) as user_is_active
        , coalesce(user_info.courserun_platform, person_course.courserun_platform) as platform
        , coalesce(user_info.user_id, person_course.user_id, s3_user.user_id) as user_id
        , coalesce(user_info.user_joined_on, s3_user.user_joined_on) as user_joined_on
    from edxorg_bigquery_user_info as user_info
    left join edxorg_bigquery_person_course as person_course on user_info.user_id = person_course.user_id
    left join edxorg_bigquery_s3_user as s3_user on user_info.user_email = s3_user.user_email
)

, combined_users as (
    select
        {{ dbt_utils.generate_surrogate_key(['mitxonline_user_view.user_email']) }} as user_pk
        , mitxonline_openedx_users.openedx_user_id as mitxonline_openedx_user_id
        , mitxonline_user_view.user_id as mitxonline_application_user_id
        , mitxonline_user_view.user_username as user_mitxonline_username
        , null as mitxpro_openedx_user_id
        , null as mitxpro_application_user_id
        , null as user_mitxpro_username
        , null as residential_openedx_user_id
        , null as residential_application_user_id
        , null as user_residential_username
        , null as edxorg_openedx_user_id
        , null as edxorg_application_user_id
        , null as user_edxorg_username
        , mitxonline_user_view.user_email as email
        , mitxonline_user_view.user_full_name as full_name
        , mitxonline_user_view.user_address_country as address_country
        , mitxonline_user_view.user_highest_education as highest_education
        , mitxonline_user_view.user_gender as gender
        , mitxonline_user_view.user_birth_year as birth_year
        , mitxonline_user_view.user_company as company
        , mitxonline_user_view.user_job_title as job_title
        , mitxonline_user_view.user_industry as industry
        , mitxonline_user_view.user_is_active
        , mitxonline_user_view.user_joined_on
    from mitxonline_user_view
    left join mitxonline_openedx_users
        on (
            mitxonline_user_view.user_username = mitxonline_openedx_users.user_username
            or mitxonline_user_view.user_email = mitxonline_openedx_users.user_email
        )

    union all

    select
        {{ dbt_utils.generate_surrogate_key(['mitxpro_user_view.user_email']) }} as user_pk
        , mitxpro_openedx_users.openedx_user_id as mitxpro_openedx_user_id
        , mitxpro_user_view.user_id as mitxpro_application_user_id
        , mitxpro_user_view.user_username as user_mitxpro_username
        , null as mitxonline_openedx_user_id
        , null as mitxonline_application_user_id
        , null as user_mitxonline_username
        , null as residential_openedx_user_id
        , null as residential_application_user_id
        , null as user_residential_username
        , null as edxorg_openedx_user_id
        , null as edxorg_application_user_id
        , null as user_edxorg_username
        , mitxpro_user_view.user_email as email
        , mitxpro_user_view.user_full_name as full_name
        , mitxpro_user_view.user_address_country as address_country
        , mitxpro_user_view.user_highest_education as highest_education
        , mitxpro_user_view.user_gender as gender
        , mitxpro_user_view.user_birth_year as birth_year
        , mitxpro_user_view.user_company as company
        , mitxpro_user_view.user_job_title as job_title
        , mitxpro_user_view.user_industry as industry
        , mitxpro_user_view.user_is_active
        , mitxpro_user_view.user_joined_on
    from mitxpro_user_view
    left join mitxpro_openedx_users
        on (
            mitxpro_user_view.user_username = mitxpro_openedx_users.user_username
            or mitxpro_user_view.user_email = mitxpro_openedx_users.user_email
        )

    union all

    select
        {{ dbt_utils.generate_surrogate_key(['mitxresidential_user_view.user_email']) }} as user_pk
        , mitxresidential_user_view.user_id as residential_openedx_user_id
        , mitxresidential_user_view.user_id as residential_application_user_id
        , mitxresidential_user_view.user_username as user_residential_username
        , null as mitxonline_openedx_user_id
        , null as mitxonline_application_user_id
        , null as user_mitxonline_username
        , null as mitxpro_openedx_user_id
        , null as mitxpro_application_user_id
        , null as user_mitxpro_username
        , null as edxorg_openedx_user_id
        , null as edxorg_application_user_id
        , null as user_edxorg_username
        , mitxresidential_user_view.user_email as email
        , mitxresidential_user_view.user_full_name as full_name
        , mitxresidential_user_view.user_address_country as address_country
        , mitxresidential_user_view.user_highest_education as highest_education
        , mitxresidential_user_view.user_gender as gender
        , mitxresidential_user_view.user_birth_year as birth_year
        , null as company
        , null as job_title
        , null as industry
        , mitxresidential_user_view.user_is_active
        , mitxresidential_user_view.user_joined_on
    from mitxresidential_user_view

    union all

    select
        {{ dbt_utils.generate_surrogate_key(['edxorg_user_view.user_email']) }} as user_pk
        , null as mitxonline_openedx_user_id
        , null as mitxonline_application_user_id
        , null as user_mitxonline_username
        , null as mitxpro_openedx_user_id
        , null as mitxpro_application_user_id
        , null as user_mitxpro_username
        , null as residential_openedx_user_id
        , null as residential_application_user_id
        , null as user_residential_username
        , edxorg_user_view.user_id as edxorg_openedx_user_id
        , edxorg_user_view.user_id as edxorg_application_user_id
        , edxorg_user_view.user_username as user_edxorg_username
        , edxorg_user_view.user_email as email
        , edxorg_user_view.user_full_name as full_name
        , edxorg_user_view.user_address_country as address_country
        , edxorg_user_view.user_highest_education as highest_education
        , edxorg_user_view.user_gender as gender
        , edxorg_user_view.user_birth_year as birth_year
        , null as company
        , null as job_title
        , null as industry
        , edxorg_user_view.user_is_active
        , edxorg_user_view.user_joined_on
    from edxorg_user_view
)

, ranked_users as (
    select
        user_pk
        , max(mitxonline_openedx_user_id) as mitxonline_openedx_user_id
        , max(mitxonline_application_user_id) as mitxonline_application_user_id
        , max(user_mitxonline_username) as user_mitxonline_username
        , max(mitxpro_openedx_user_id) as mitxpro_openedx_user_id
        , max(mitxpro_application_user_id) as mitxpro_application_user_id
        , max(user_mitxpro_username) as user_mitxpro_username
        , max(residential_openedx_user_id) as residential_openedx_user_id
        , max(residential_application_user_id) as residential_application_user_id
        , max(user_residential_username) as user_residential_username
        , max(edxorg_openedx_user_id) as edxorg_openedx_user_id
        , max(edxorg_application_user_id) as edxorg_application_user_id
        , max(user_edxorg_username) as user_edxorg_username
        , max(email) as email
        , max(full_name) as full_name
        , max(address_country) as address_country
        , max(highest_education) as highest_education
        , max(gender) as gender
        , max(birth_year) as birth_year
        , max(company) as company
        , max(job_title) as job_title
        , max(industry) as industry
        , max(user_is_active) as user_is_active
        , max(user_joined_on) as user_joined_on
        , row_number() over (partition by user_pk order by max(user_joined_on) desc) as row_num
    from combined_users
    group by user_pk
)

select *
from ranked_users
where row_num = 1
