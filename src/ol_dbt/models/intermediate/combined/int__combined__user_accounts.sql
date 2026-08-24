-- One row per SOURCE-SYSTEM ACCOUNT across every platform, before any identity
-- resolution. Extracted verbatim from dim_user.sql's `combined_accounts` CTE so that
-- int__combined__user_key_map and dim_user resolve identity from the same account list
-- instead of each rebuilding it.
--
-- NOT int__combined__users. That model implements a competing identity rule (dedup on
-- coalesce(username, id, email, full_name) per platform) and feeds the marts layer. This
-- one feeds the dimensional layer. Retiring the duplication is tracked separately; until
-- then, do not use them interchangeably.
--
-- Grain: one row per (id_source, id_source_user_id), or per email for the rows that carry
-- no source id at all (Emeritus and Global Alumni pre-date stable ids). `id_source` is an
-- ID NAMESPACE, not a platform: the same integer means different people in mitxonline's
-- application and open edX id spaces, which is why they rank separately downstream.
{{ config(materialized='view') }}

with mitx_users as (
    select
        user_mitxonline_id as mitxonline_application_user_id
        , user_mitxonline_username
        , user_global_id
        , user_edxorg_id as edxorg_openedx_user_id
        , user_edxorg_username
        , user_mitxonline_email
        , user_edxorg_email
        , user_full_name as full_name
        , user_address_country as address_country
        , user_highest_education as highest_education
        , user_gender as gender
        , user_birth_year as birth_year
        , user_company as company
        , user_job_title as job_title
        , user_industry as industry
        , user_address_state
        , user_is_active_on_mitxonline
        , user_is_active_on_edxorg
        , user_joined_on_mitxonline
        , user_joined_on_edxorg
        , case
            when user_is_active_on_mitxonline and user_joined_on_mitxonline > user_joined_on_edxorg
                then user_mitxonline_email
            else coalesce(user_edxorg_email, user_mitxonline_email, user_micromasters_email)
        end as user_email
        , user_micromasters_id
    from {{ ref('int__mitx__users') }}
)

, mitlearn_openedx_users as (
    select
        openedx_user_id
        , user_username
        , user_email
    from {{ ref('stg__mitxonline__openedx__mysql__auth_user') }}
)

, mitxonline_app_openedxuser_mapping as (
    select * from {{ ref('stg__mitxonline__app__postgres__openedx_openedxuser') }}
)

-- MITx Pro Users
, mitxpro_users as (
    select
        user_id
        , user_username
        , user_email
        , user_full_name
        , user_is_active
        , user_joined_on
    from {{ ref('stg__mitxpro__app__postgres__users_user') }}
)

, mitxpro_legaladdress as (
    select
        user_id
        , user_address_country
        , user_address_state_or_territory
    from {{ ref('stg__mitxpro__app__postgres__users_legaladdress') }}
)

, mitxpro_profile as (
    select
        user_id
        , user_highest_education
        , user_gender
        , user_birth_year
        , user_company
        , user_job_title
        , user_industry
    from {{ ref('stg__mitxpro__app__postgres__users_profile') }}
)

, mitxpro_openedx_users as (
    select
        openedx_user_id
        , user_username
        , user_email
    from {{ ref('stg__mitxpro__openedx__mysql__auth_user') }}
)

, mitxpro_user_view as (
    select
        mitxpro_users.user_username
        , mitxpro_users.user_email
        , mitxpro_users.user_full_name
        , mitxpro_legaladdress.user_address_country
        , mitxpro_legaladdress.user_address_state_or_territory
        , mitxpro_profile.user_highest_education
        , mitxpro_profile.user_gender
        , mitxpro_profile.user_birth_year
        , mitxpro_profile.user_company
        , mitxpro_profile.user_job_title
        , mitxpro_profile.user_industry
        , 'mitxpro' as platform
        , mitxpro_users.user_id
        , mitxpro_users.user_is_active
        , mitxpro_users.user_joined_on
    from mitxpro_users
    left join mitxpro_legaladdress on mitxpro_users.user_id = mitxpro_legaladdress.user_id
    left join mitxpro_profile on mitxpro_users.user_id = mitxpro_profile.user_id
)

, emeritus_users as (
    select * from (
        select
            user_id
            , user_email
            , user_full_name
            , user_address_country
            , user_gender
            , user_company
            , user_job_title
            , user_industry
            , row_number() over (
                partition by coalesce(user_id, user_email, user_full_name)
                order by user_gdpr_consent_date desc, enrollment_created_on desc
            ) as row_num
        from {{ ref('stg__emeritus__api__bigquery__user_enrollments') }}
    )
    where row_num = 1
)

, global_alumni_users as (
    select * from (
        select
            user_id
            , user_email
            , user_full_name
            , user_address_country
            , user_gender
            , user_company
            , user_job_title
            , user_industry
            , row_number() over (
                partition by user_email
                order by user_gdpr_consent_date desc, courserun_start_on desc
            ) as row_num
        from {{ ref('stg__global_alumni__api__bigquery__user_enrollments') }}
    )
    where row_num = 1
)

-- Residential Users
, mitxresidential_openedx_users as (
    select
        user_username
        , user_email
        , user_full_name
        , user_is_active
        , user_id
        , user_joined_on
    from {{ ref('stg__mitxresidential__openedx__auth_user') }}
)

, mitxresidential_profile as (
    select
        user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year
        , user_id
    from {{ ref('stg__mitxresidential__openedx__auth_userprofile') }}
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
        , 'residential' as platform
        , mitxresidential_openedx_users.user_id
        , mitxresidential_openedx_users.user_is_active
        , mitxresidential_openedx_users.user_joined_on
    from mitxresidential_openedx_users
    left join mitxresidential_profile on mitxresidential_openedx_users.user_id = mitxresidential_profile.user_id
)

, bootcamps_user_view as (
    select * from {{ ref('int__bootcamps__users') }}
)

, mitx_users_view as (
    select
        mitx_users.user_global_id
        , mitlearn_openedx_users.openedx_user_id as mitlearn_openedx_user_id
        , mitlearn_openedx_users.openedx_user_id as mitxonline_openedx_user_id
        , mitx_users.mitxonline_application_user_id
        , coalesce(
            mitxonline_app_openedxuser_mapping.openedxuser_username
            , mitx_users.user_mitxonline_username
            , mitlearn_openedx_users.user_username
        ) as user_mitxonline_username
        , mitx_users.edxorg_openedx_user_id
        , mitx_users.user_edxorg_username
        , coalesce(mitx_users.user_email, mitlearn_openedx_users.user_email) as email
        , mitx_users.full_name
        , mitx_users.address_country
        , mitx_users.highest_education
        , mitx_users.gender
        , mitx_users.birth_year
        , mitx_users.company
        , mitx_users.job_title
        , mitx_users.industry
        , mitx_users.user_address_state
        , mitx_users.user_is_active_on_mitxonline
        , mitx_users.user_joined_on_mitxonline
        , mitx_users.user_is_active_on_edxorg
        , mitx_users.user_joined_on_edxorg
        , mitx_users.user_micromasters_id
    from mitx_users
    left join mitxonline_app_openedxuser_mapping
        on mitx_users.mitxonline_application_user_id = mitxonline_app_openedxuser_mapping.user_id
    full outer join mitlearn_openedx_users
        on mitxonline_app_openedxuser_mapping.openedxuser_username = mitlearn_openedx_users.user_username
)

, learn_user_deduped_by_email as (
    select * from (
        select
            *
            , row_number() over (
                partition by user_email
                order by user_created_on desc
            ) as row_num
        from {{ ref('stg__mitlearn__app__postgres__users_user') }}
    )
    where row_num = 1
)

-- Learn can hold two accounts per user_global_id, which fans out the join to MITx below.
-- nulls last: Trino sorts nulls largest, ranking never-logged-in accounts first.
, learn_user as (
    select * from (
        select
            *
            , row_number() over (
                partition by user_global_id, case when user_global_id is null then user_id end
                order by user_last_login desc nulls last, user_created_on desc nulls last, user_id desc
            ) as global_id_row_num
        from learn_user_deduped_by_email
    )
    where global_id_row_num = 1
)

, learn_profile as (
    select * from {{ ref('stg__mitlearn__app__postgres__profiles_profile') }}
)
, learn_user_view as(
    select
        learn_user.user_global_id
        , learn_user.user_id as mitlearn_user_id
        , learn_user.user_email as email
        , case when learn_profile.user_name is not null and trim(learn_profile.user_name) <> ''
            then learn_profile.user_name
            else concat(learn_user.user_first_name, ' ', learn_user.user_last_name)
        end as full_name
        , learn_profile.user_current_education as highest_education
        , learn_user.user_is_active as user_is_active_on_mitlearn
        , learn_user.user_joined_on as user_joined_on_mitlearn
    from learn_user
    left join learn_profile on learn_user.user_id = learn_profile.user_id
)

, users_with_global_id as (
    select
        learn_user_view.mitlearn_user_id
        , mitx_users_view.mitlearn_openedx_user_id
        , mitx_users_view.mitxonline_openedx_user_id
        , mitx_users_view.mitxonline_application_user_id
        , mitx_users_view.user_mitxonline_username
        , mitx_users_view.edxorg_openedx_user_id
        , mitx_users_view.user_edxorg_username
        , mitx_users_view.address_country
        , mitx_users_view.gender
        , mitx_users_view.birth_year
        , mitx_users_view.company
        , mitx_users_view.job_title
        , mitx_users_view.industry
        , mitx_users_view.user_address_state
        , learn_user_view.user_is_active_on_mitlearn
        , learn_user_view.user_joined_on_mitlearn
        , mitx_users_view.user_is_active_on_mitxonline
        , mitx_users_view.user_joined_on_mitxonline
        , mitx_users_view.user_is_active_on_edxorg
        , mitx_users_view.user_joined_on_edxorg
        , coalesce(learn_user_view.full_name, mitx_users_view.full_name) as full_name
        , coalesce(learn_user_view.user_global_id, mitx_users_view.user_global_id) as user_global_id
        , coalesce(learn_user_view.highest_education, mitx_users_view.highest_education) as highest_education
        , coalesce(
            case
                when mitx_users_view.user_is_active_on_mitxonline
                    and mitx_users_view.user_joined_on_mitxonline > learn_user_view.user_joined_on_mitlearn
                then mitx_users_view.email
            end,
            learn_user_view.email,
            mitx_users_view.email
        ) as email
        , mitx_users_view.user_micromasters_id
    from mitx_users_view
             full outer join learn_user_view on mitx_users_view.user_global_id = learn_user_view.user_global_id
)

-- id_source is an id namespace, not a platform: the same integer means different people
-- in mitxonline's application and open edX id spaces.
, combined_accounts as (
    select
        case
            when mitlearn_user_id is not null then 'mitlearn'
            when mitxonline_application_user_id is not null then 'mitxonline'
            when edxorg_openedx_user_id is not null then 'edxorg'
            when user_micromasters_id is not null then 'micromasters'
            when mitxonline_openedx_user_id is not null then 'mitxonline_openedx'
        end as id_source
        , coalesce(
            cast(mitlearn_user_id as varchar)
            , cast(mitxonline_application_user_id as varchar)
            , cast(edxorg_openedx_user_id as varchar)
            , cast(user_micromasters_id as varchar)
            , cast(mitxonline_openedx_user_id as varchar)
        ) as id_source_user_id
        , user_global_id
        , mitlearn_user_id
        , mitlearn_openedx_user_id
        , mitxonline_openedx_user_id
        , mitxonline_application_user_id
        , user_mitxonline_username
        , null as mitxpro_openedx_user_id
        , null as mitxpro_application_user_id
        , null as user_mitxpro_username
        , null as residential_openedx_user_id
        , null as user_residential_username
        , edxorg_openedx_user_id
        , user_edxorg_username
        , null as emeritus_user_id
        , null as global_alumni_user_id
        , lower(email) as email
        , full_name
        , address_country
        , highest_education
        , gender
        , birth_year
        , company
        , job_title
        , industry
        , user_address_state as address_state
        , user_is_active_on_mitlearn
        , user_joined_on_mitlearn
        , user_is_active_on_mitxonline
        , user_joined_on_mitxonline
        , user_is_active_on_edxorg
        , user_joined_on_edxorg
        , null as user_is_active_on_mitxpro
        , null as user_joined_on_mitxpro
        , null as user_is_active_on_residential
        , null as user_joined_on_residential
        , user_micromasters_id as micromasters_user_id
        , null as bootcamps_application_user_id
        , null as user_is_active_on_bootcamps
        , null as user_joined_on_bootcamps
    from users_with_global_id
    where email is not null

    union all

    select
        'mitxpro' as id_source
        , cast(mitxpro_user_view.user_id as varchar) as id_source_user_id
        , null as user_global_id
        , null as mitlearn_user_id
        , null as mitlearn_openedx_user_id
        , null as mitxonline_openedx_user_id
        , null as mitxonline_application_user_id
        , null as user_mitxonline_username
        , coalesce(
            openedx_users_username.openedx_user_id, openedx_users_email.openedx_user_id
        ) as mitxpro_openedx_user_id
        , mitxpro_user_view.user_id as mitxpro_application_user_id
        , mitxpro_user_view.user_username as user_mitxpro_username
        , null as residential_openedx_user_id
        , null as user_residential_username
        , null as edxorg_openedx_user_id
        , null as user_edxorg_username
        , null as emeritus_user_id
        , null as global_alumni_user_id
        , lower(mitxpro_user_view.user_email) as email
        , mitxpro_user_view.user_full_name as full_name
        , mitxpro_user_view.user_address_country as address_country
        , mitxpro_user_view.user_highest_education as highest_education
        , mitxpro_user_view.user_gender as gender
        , mitxpro_user_view.user_birth_year as birth_year
        , mitxpro_user_view.user_company as company
        , mitxpro_user_view.user_job_title as job_title
        , mitxpro_user_view.user_industry as industry
        , mitxpro_user_view.user_address_state_or_territory as address_state
        , null as user_is_active_on_mitlearn
        , null as user_joined_on_mitlearn
        , null as user_is_active_on_mitxonline
        , null as user_joined_on_mitxonline
        , null as user_is_active_on_edxorg
        , null as user_joined_on_edxorg
        , mitxpro_user_view.user_is_active as user_is_active_on_mitxpro
        , mitxpro_user_view.user_joined_on as user_joined_on_mitxpro
        , null as user_is_active_on_residential
        , null as user_joined_on_residential
        , null as micromasters_user_id
        , null as bootcamps_application_user_id
        , null as user_is_active_on_bootcamps
        , null as user_joined_on_bootcamps
    from mitxpro_user_view
    left join mitxpro_openedx_users as openedx_users_username
        on mitxpro_user_view.user_username = openedx_users_username.user_username
    left join mitxpro_openedx_users as openedx_users_email
        on lower(mitxpro_user_view.user_email) = lower(openedx_users_email.user_email)
    where mitxpro_user_view.user_email is not null

    union all

    select
        'emeritus' as id_source
        -- null where Emeritus has no user_id: these accounts get keyed off the email below
        , cast(user_id as varchar) as id_source_user_id
        , null as user_global_id
        , null as mitlearn_user_id
        , null as mitlearn_openedx_user_id
        , null as mitxonline_openedx_user_id
        , null as mitxonline_application_user_id
        , null as user_mitxonline_username
        , null as mitxpro_openedx_user_id
        , null as mitxpro_application_user_id
        , null as user_mitxpro_username
        , null as residential_openedx_user_id
        , null as user_residential_username
        , null as edxorg_openedx_user_id
        , null as user_edxorg_username
        , user_id as emeritus_user_id
        , null as global_alumni_user_id
        , lower(user_email) as email
        , user_full_name as full_name
        , user_address_country as address_country
        , null as highest_education
        , user_gender as gender
        , null as birth_year
        , user_company as company
        , user_job_title as job_title
        , user_industry as industry
        , null as address_state
        , null as user_is_active_on_mitlearn
        , null as user_joined_on_mitlearn
        , null as user_is_active_on_mitxonline
        , null as user_joined_on_mitxonline
        , null as user_is_active_on_edxorg
        , null as user_joined_on_edxorg
        , null as user_is_active_on_mitxpro
        , null as user_joined_on_mitxpro
        , null as user_is_active_on_residential
        , null as user_joined_on_residential
        , null as micromasters_user_id
        , null as bootcamps_application_user_id
        , null as user_is_active_on_bootcamps
        , null as user_joined_on_bootcamps
    from emeritus_users
    where user_email is not null

    union all

    select
        'global_alumni' as id_source
        , cast(user_id as varchar) as id_source_user_id
        , null as user_global_id
        , null as mitlearn_user_id
        , null as mitlearn_openedx_user_id
        , null as mitxonline_openedx_user_id
        , null as mitxonline_application_user_id
        , null as user_mitxonline_username
        , null as mitxpro_openedx_user_id
        , null as mitxpro_application_user_id
        , null as user_mitxpro_username
        , null as residential_openedx_user_id
        , null as user_residential_username
        , null as edxorg_openedx_user_id
        , null as user_edxorg_username
        , null as emeritus_user_id
        , user_id as global_alumni_user_id
        , lower(user_email) as email
        , user_full_name as full_name
        , user_address_country as address_country
        , null as highest_education
        , user_gender as gender
        , null as birth_year
        , user_company as company
        , user_job_title as job_title
        , user_industry as industry
        , null as address_state
        , null as user_is_active_on_mitlearn
        , null as user_joined_on_mitlearn
        , null as user_is_active_on_mitxonline
        , null as user_joined_on_mitxonline
        , null as user_is_active_on_edxorg
        , null as user_joined_on_edxorg
        , null as user_is_active_on_mitxpro
        , null as user_joined_on_mitxpro
        , null as user_is_active_on_residential
        , null as user_joined_on_residential
        , null as micromasters_user_id
        , null as bootcamps_application_user_id
        , null as user_is_active_on_bootcamps
        , null as user_joined_on_bootcamps
    from global_alumni_users
    where user_email is not null

    union all

    select
        'residential' as id_source
        , cast(mitxresidential_user_view.user_id as varchar) as id_source_user_id
        , null as user_global_id
        , null as mitlearn_user_id
        , null as mitlearn_openedx_user_id
        , null as mitxonline_openedx_user_id
        , null as mitxonline_application_user_id
        , null as user_mitxonline_username
        , null as mitxpro_openedx_user_id
        , null as mitxpro_application_user_id
        , null as user_mitxpro_username
        , mitxresidential_user_view.user_id as residential_openedx_user_id
        , mitxresidential_user_view.user_username as user_residential_username
        , null as edxorg_openedx_user_id
        , null as user_edxorg_username
        , null as emeritus_user_id
        , null as global_alumni_user_id
        , lower(mitxresidential_user_view.user_email) as email
        , mitxresidential_user_view.user_full_name as full_name
        , mitxresidential_user_view.user_address_country as address_country
        , mitxresidential_user_view.user_highest_education as highest_education
        , mitxresidential_user_view.user_gender as gender
        , mitxresidential_user_view.user_birth_year as birth_year
        , null as company
        , null as job_title
        , null as industry
        , null as address_state
        , null as user_is_active_on_mitlearn
        , null as user_joined_on_mitlearn
        , null as user_is_active_on_mitxonline
        , null as user_joined_on_mitxonline
        , null as user_is_active_on_edxorg
        , null as user_joined_on_edxorg
        , null as user_is_active_on_mitxpro
        , null as user_joined_on_mitxpro
        , mitxresidential_user_view.user_is_active as user_is_active_on_residential
        , mitxresidential_user_view.user_joined_on as user_joined_on_residential
        , null as micromasters_user_id
        , null as bootcamps_application_user_id
        , null as user_is_active_on_bootcamps
        , null as user_joined_on_bootcamps
    from mitxresidential_user_view
    where mitxresidential_user_view.user_email is not null

    union all

    select
        'bootcamps' as id_source
        , cast(bootcamps_user_view.user_id as varchar) as id_source_user_id
        , null as user_global_id
        , null as mitlearn_user_id
        , null as mitlearn_openedx_user_id
        , null as mitxonline_openedx_user_id
        , null as mitxonline_application_user_id
        , null as user_mitxonline_username
        , null as mitxpro_openedx_user_id
        , null as mitxpro_application_user_id
        , null as user_mitxpro_username
        , null as residential_openedx_user_id
        , null as user_residential_username
        , null as edxorg_openedx_user_id
        , null as user_edxorg_username
        , null as emeritus_user_id
        , null as global_alumni_user_id
        , lower(bootcamps_user_view.user_email) as email
        , bootcamps_user_view.user_full_name as full_name
        , bootcamps_user_view.user_address_country as address_country
        , bootcamps_user_view.user_highest_education as highest_education
        , bootcamps_user_view.user_gender as gender
        , bootcamps_user_view.user_birth_year as birth_year
        , bootcamps_user_view.user_company as company
        , bootcamps_user_view.user_job_title as job_title
        , bootcamps_user_view.user_industry as industry
        , bootcamps_user_view.user_address_state_or_territory as address_state
        , null as user_is_active_on_mitlearn
        , null as user_joined_on_mitlearn
        , null as user_is_active_on_mitxonline
        , null as user_joined_on_mitxonline
        , null as user_is_active_on_edxorg
        , null as user_joined_on_edxorg
        , null as user_is_active_on_mitxpro
        , null as user_joined_on_mitxpro
        , null as user_is_active_on_residential
        , null as user_joined_on_residential
        , null as micromasters_user_id
        , bootcamps_user_view.user_id as bootcamps_application_user_id
        , bootcamps_user_view.user_is_active as user_is_active_on_bootcamps
        , bootcamps_user_view.user_joined_on as user_joined_on_bootcamps
    from bootcamps_user_view
    where bootcamps_user_view.user_email is not null

)

select * from combined_accounts
