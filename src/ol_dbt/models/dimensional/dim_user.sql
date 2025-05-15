-- MITx users from MITx Online and edX with dedup
-- For users exist on both MITx Online and edX.org
with mitxonline_user_view as (
    select
        user_mitxonline_id as mitxonline_application_user_id
        , user_mitxonline_username
        , user_edxorg_id as edxorg_openedx_user_id
        , user_edxorg_username
        , user_mitxonline_email as email
        , user_full_name as full_name
        , user_address_country as address_country
        , user_highest_education as highest_education
        , user_gender as gender
        , user_birth_year as birth_year
        , user_company as company
        , user_job_title as job_title
        , user_industry as industry
        , user_is_active_on_mitxonline
        , user_is_active_on_edxorg
        , user_joined_on_mitxonline
        , user_joined_on_edxorg
        , case
            when user_is_active_on_mitxonline and user_joined_on_mitxonline > user_joined_on_edxorg
                then user_mitxonline_email
            else coalesce(user_edxorg_email, user_mitxonline_email, user_micromasters_email)
        end as user_email
    from {{ ref('int__mitx__users') }}
)

, mitxonline_openedx_users as (
    select
        openedx_user_id
        , user_username
        , user_email
    from {{ ref('stg__mitxonline__openedx__mysql__auth_user') }}
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

, combined_users as (
    select
        {{ dbt_utils.generate_surrogate_key(['mitxonline_user_view.user_email']) }} as user_pk
        , mitxonline_openedx_users.openedx_user_id as mitxonline_openedx_user_id
        , mitxonline_user_view.mitxonline_application_user_id
        , mitxonline_user_view.user_mitxonline_username
        , cast(null as bigint) as mitxpro_openedx_user_id
        , cast(null as bigint) as mitxpro_application_user_id
        , cast(null as varchar) as user_mitxpro_username
        , cast(null as bigint) as residential_openedx_user_id
        , cast(null as varchar) as user_residential_username
        , mitxonline_user_view.edxorg_openedx_user_id
        , mitxonline_user_view.user_edxorg_username
        , mitxonline_user_view.user_email as email
        , mitxonline_user_view.full_name
        , mitxonline_user_view.address_country
        , mitxonline_user_view.highest_education
        , mitxonline_user_view.gender
        , mitxonline_user_view.birth_year
        , mitxonline_user_view.company
        , mitxonline_user_view.job_title
        , mitxonline_user_view.industry
        , mitxonline_user_view.user_is_active_on_mitxonline
        , mitxonline_user_view.user_joined_on_mitxonline
        , mitxonline_user_view.user_is_active_on_edxorg
        , mitxonline_user_view.user_joined_on_edxorg
        , cast(null as boolean) as user_is_active_on_mitxpro
        , cast(null as varchar) as user_joined_on_mitxpro
        , cast(null as boolean) as user_is_active_on_residential
        , cast(null as varchar) as user_joined_on_residential
    from mitxonline_user_view
    left join mitxonline_openedx_users
        on (
            mitxonline_user_view.user_mitxonline_username = mitxonline_openedx_users.user_username
            or mitxonline_user_view.user_email = mitxonline_openedx_users.user_email
        )

    union all

    select
        {{ dbt_utils.generate_surrogate_key(['mitxpro_user_view.user_email']) }} as user_pk
        , cast(null as bigint) as mitxonline_openedx_user_id
        , cast(null as bigint) as mitxonline_application_user_id
        , cast(null as varchar) as user_mitxonline_username
        , mitxpro_openedx_users.openedx_user_id as mitxpro_openedx_user_id
        , mitxpro_user_view.user_id as mitxpro_application_user_id
        , mitxpro_user_view.user_username as user_mitxpro_username
        , cast(null as bigint) as residential_openedx_user_id
        , cast(null as varchar) as user_residential_username
        , cast(null as bigint) as edxorg_openedx_user_id
        , cast(null as varchar) as user_edxorg_username
        , mitxpro_user_view.user_email as email
        , mitxpro_user_view.user_full_name as full_name
        , mitxpro_user_view.user_address_country as address_country
        , mitxpro_user_view.user_highest_education as highest_education
        , mitxpro_user_view.user_gender as gender
        , mitxpro_user_view.user_birth_year as birth_year
        , mitxpro_user_view.user_company as company
        , mitxpro_user_view.user_job_title as job_title
        , mitxpro_user_view.user_industry as industry
        , cast(null as boolean) as user_is_active_on_mitxonline
        , cast(null as varchar) as user_joined_on_mitxonline
        , cast(null as boolean) as user_is_active_on_edxorg
        , cast(null as varchar) as user_joined_on_edxorg
        , mitxpro_user_view.user_is_active as user_is_active_on_mitxpro
        , mitxpro_user_view.user_joined_on as user_joined_on_mitxpro
        , cast(null as boolean) as user_is_active_on_residential
        , cast(null as varchar) as user_joined_on_residential
    from mitxpro_user_view
    left join mitxpro_openedx_users
        on (
            mitxpro_user_view.user_username = mitxpro_openedx_users.user_username
            or mitxpro_user_view.user_email = mitxpro_openedx_users.user_email
        )

    union all

    select
        {{ dbt_utils.generate_surrogate_key(['mitxresidential_user_view.user_email']) }} as user_pk
        , cast(null as bigint) as mitxonline_openedx_user_id
        , cast(null as bigint) as mitxonline_application_user_id
        , cast(null as varchar) as user_mitxonline_username
        , cast(null as bigint) as mitxpro_openedx_user_id
        , cast(null as bigint) as mitxpro_application_user_id
        , cast(null as varchar) as user_mitxpro_username
        , mitxresidential_user_view.user_id as residential_openedx_user_id
        , mitxresidential_user_view.user_username as user_residential_username
        , cast(null as bigint) as edxorg_openedx_user_id
        , cast(null as varchar) as user_edxorg_username
        , mitxresidential_user_view.user_email as email
        , mitxresidential_user_view.user_full_name as full_name
        , mitxresidential_user_view.user_address_country as address_country
        , mitxresidential_user_view.user_highest_education as highest_education
        , mitxresidential_user_view.user_gender as gender
        , mitxresidential_user_view.user_birth_year as birth_year
        , cast(null as varchar) as company
        , cast(null as varchar) as job_title
        , cast(null as varchar) as industry
        , cast(null as boolean) as user_is_active_on_mitxonline
        , cast(null as varchar) as user_joined_on_mitxonline
        , cast(null as boolean) as user_is_active_on_edxorg
        , cast(null as varchar) as user_joined_on_edxorg
        , cast(null as boolean) as user_is_active_on_mitxpro
        , cast(null as varchar) as user_joined_on_mitxpro
        , mitxresidential_user_view.user_is_active as user_is_active_on_residential
        , mitxresidential_user_view.user_joined_on as user_joined_on_residential
    from mitxresidential_user_view
)

, ranked_users as (
    select
        *
        , row_number() over (
            partition by user_pk
            order by
                greatest(
                    user_joined_on_mitxonline
                    , user_joined_on_edxorg
                    , user_joined_on_mitxpro
                    , user_joined_on_residential
                ) desc
        ) as row_num
    from combined_users
)

, base_info as (
    select
        user_pk
        , email
        , full_name
        , address_country
    from ranked_users
    where row_num = 1
)

, id_agg as (
    select
        user_pk
        , max(mitxonline_openedx_user_id) as mitxonline_openedx_user_id
        , max(mitxonline_application_user_id) as mitxonline_application_user_id
        , max(user_mitxonline_username) as user_mitxonline_username
        , max(mitxpro_openedx_user_id) as mitxpro_openedx_user_id
        , max(mitxpro_application_user_id) as mitxpro_application_user_id
        , max(user_mitxpro_username) as user_mitxpro_username
        , max(residential_openedx_user_id) as residential_openedx_user_id
        , max(user_residential_username) as user_residential_username
        , max(edxorg_openedx_user_id) as edxorg_openedx_user_id
        , max(user_edxorg_username) as user_edxorg_username
        , max(highest_education) as highest_education
        , max(gender) as gender
        , max(birth_year) as birth_year
        , max(company) as company
        , max(job_title) as job_title
        , max(industry) as industry
        , max(user_is_active_on_mitxonline) as user_is_active_on_mitxonline
        , max(user_joined_on_mitxonline) as user_joined_on_mitxonline
        , max(user_is_active_on_edxorg) as user_is_active_on_edxorg
        , max(user_joined_on_edxorg) as user_joined_on_edxorg
        , max(user_is_active_on_mitxpro) as user_is_active_on_mitxpro
        , max(user_joined_on_mitxpro) as user_joined_on_mitxpro
        , max(user_is_active_on_residential) as user_is_active_on_residential
        , max(user_joined_on_residential) as user_joined_on_residential
    from combined_users
    group by user_pk
)

select
    base.user_pk
    , agg.mitxonline_openedx_user_id
    , agg.mitxonline_application_user_id
    , agg.user_mitxonline_username
    , agg.mitxpro_openedx_user_id
    , agg.mitxpro_application_user_id
    , agg.user_mitxpro_username
    , agg.residential_openedx_user_id
    , agg.user_residential_username
    , agg.edxorg_openedx_user_id
    , agg.user_edxorg_username
    , base.email
    , base.full_name
    , base.address_country
    , agg.highest_education
    , agg.gender
    , agg.birth_year
    , agg.company
    , agg.job_title
    , agg.industry
    , agg.user_is_active_on_mitxonline
    , agg.user_joined_on_mitxonline
    , agg.user_is_active_on_edxorg
    , agg.user_joined_on_edxorg
    , agg.user_is_active_on_mitxpro
    , agg.user_joined_on_mitxpro
    , agg.user_is_active_on_residential
    , agg.user_joined_on_residential
from base_info as base
inner join id_agg as agg on base.user_pk = agg.user_pk
