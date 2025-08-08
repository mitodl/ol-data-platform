-- MITx users from MITx Online and edX with dedup
-- For users exist on both MITx Online and edX.org
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

, mitx_users_view as (
    select
        mitx_users.user_global_id
        , coalesce(
            openedx_users_username.openedx_user_id, openedx_users_email.openedx_user_id
        ) as mitxonline_openedx_user_id
        , mitx_users.mitxonline_application_user_id
        , mitx_users.user_mitxonline_username
        , mitx_users.edxorg_openedx_user_id
        , mitx_users.user_edxorg_username
        , mitx_users.user_email as email
        , mitx_users.full_name
        , mitx_users.address_country
        , mitx_users.highest_education
        , mitx_users.gender
        , mitx_users.birth_year
        , mitx_users.company
        , mitx_users.job_title
        , mitx_users.industry
        , mitx_users.user_is_active_on_mitxonline
        , mitx_users.user_joined_on_mitxonline
        , mitx_users.user_is_active_on_edxorg
        , mitx_users.user_joined_on_edxorg
    from mitx_users
    left join mitxonline_openedx_users as openedx_users_username
        on mitx_users.user_mitxonline_username = openedx_users_username.user_username
    left join mitxonline_openedx_users as openedx_users_email
        on mitx_users.user_mitxonline_email = openedx_users_email.user_email
)

, learn_user as (
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

, learn_profile as (
    select * from {{ ref('stg__mitlearn__app__postgres__profiles_profile') }}
)

, learn_user_topic_interests as (
    select
        profile_topic_interests.profile_id
        , array_agg(topic.learningresourcetopic_name) as topic_interests
    from {{ ref('stg__mitlearn__app__postgres__profiles_profile_topic_interests') }} as profile_topic_interests
    join {{ ref('stg__mitlearn__app__postgres__learning_resources_learningresourcetopic') }} as topic
        on profile_topic_interests.learningresourcetopic_id = topic.learningresourcetopic_id
    group by profile_topic_interests.profile_id

)

, learn_user_view as(
    select
        learn_user.user_global_id
        , learn_user.user_id as mitlearn_user_id
        , learn_user.user_email as email
        , concat(learn_user.user_first_name, ' ', learn_user.user_last_name) as full_name
        , learn_profile.user_current_education as highest_education
        , learn_user.user_is_active as user_is_active_on_mitlearn
        , learn_user.user_joined_on as user_joined_on_mitlearn
    from learn_user
    left join learn_profile on learn_user.user_id = learn_profile.user_id
)

, users_with_global_id as (
    select
        learn_user_view.mitlearn_user_id
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
    from mitx_users_view
             full outer join learn_user_view on mitx_users_view.user_global_id = learn_user_view.user_global_id
)

, combined_users as (
    select
        {{ dbt_utils.generate_surrogate_key(['email']) }} as user_pk
        , user_global_id
        , mitlearn_user_id
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
        , email
        , full_name
        , address_country
        , highest_education
        , gender
        , birth_year
        , company
        , job_title
        , industry
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
    from users_with_global_id

    union all

    select
        {{ dbt_utils.generate_surrogate_key(['mitxpro_user_view.user_email']) }} as user_pk
        , null as user_global_id
        , null as mitlearn_user_id
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
        , mitxpro_user_view.user_email as email
        , mitxpro_user_view.user_full_name as full_name
        , mitxpro_user_view.user_address_country as address_country
        , mitxpro_user_view.user_highest_education as highest_education
        , mitxpro_user_view.user_gender as gender
        , mitxpro_user_view.user_birth_year as birth_year
        , mitxpro_user_view.user_company as company
        , mitxpro_user_view.user_job_title as job_title
        , mitxpro_user_view.user_industry as industry
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
    from mitxpro_user_view
    left join mitxpro_openedx_users as openedx_users_username
        on mitxpro_user_view.user_username = openedx_users_username.user_username
    left join mitxpro_openedx_users as openedx_users_email
        on mitxpro_user_view.user_email = openedx_users_email.user_email

    union all

    select
        {{ dbt_utils.generate_surrogate_key(['user_email']) }} as user_pk
        , null as user_global_id
        , null as mitlearn_user_id
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
        , user_email as email
        , user_full_name as full_name
        , user_address_country as address_country
        , null as highest_education
        , user_gender as gender
        , null as birth_year
        , user_company as company
        , user_job_title as job_title
        , user_industry as industry
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
    from emeritus_users
    where user_email is not null

    union all

    select
        {{ dbt_utils.generate_surrogate_key(['user_email']) }} as user_pk
        , null as user_global_id
        , null as mitlearn_user_id
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
        , user_email as email
        , user_full_name as full_name
        , user_address_country as address_country
        , null as highest_education
        , user_gender as gender
        , null as birth_year
        , user_company as company
        , user_job_title as job_title
        , user_industry as industry
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
    from global_alumni_users
    where user_email is not null

    union all

    select
        {{ dbt_utils.generate_surrogate_key(['mitxresidential_user_view.user_email']) }} as user_pk
        , null as user_global_id
        , null as mitlearn_user_id
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
        , mitxresidential_user_view.user_email as email
        , mitxresidential_user_view.user_full_name as full_name
        , mitxresidential_user_view.user_address_country as address_country
        , mitxresidential_user_view.user_highest_education as highest_education
        , mitxresidential_user_view.user_gender as gender
        , mitxresidential_user_view.user_birth_year as birth_year
        , null as company
        , null as job_title
        , null as industry
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
    select *
    from ranked_users
    where row_num = 1
)

, agg_view as (
    select
        user_pk
        , max(user_global_id) as user_global_id
        , max(mitlearn_user_id) as mitlearn_user_id
        , max(mitxonline_openedx_user_id) as mitxonline_openedx_user_id
        , max(mitxonline_application_user_id) as mitxonline_application_user_id
        , max(user_mitxonline_username) as user_mitxonline_username
        , max(mitxpro_openedx_user_id) as mitxpro_openedx_user_id
        , max(mitxpro_application_user_id) as mitxpro_application_user_id
        , max(user_mitxpro_username) as user_mitxpro_username
        , max(residential_openedx_user_id) as residential_openedx_user_id
        , max(user_residential_username) as user_residential_username
        , max(edxorg_openedx_user_id) as edxorg_openedx_user_id
        , max(emeritus_user_id) as emeritus_user_id
        , max(global_alumni_user_id) as global_alumni_user_id
        , max(user_edxorg_username) as user_edxorg_username
        , max(user_is_active_on_mitlearn) as user_is_active_on_mitlearn
        , max(user_joined_on_mitlearn) as user_joined_on_mitlearn
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
    , agg.user_global_id
    , agg.mitlearn_user_id
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
    , agg.emeritus_user_id
    , agg.global_alumni_user_id
    , base.email
    , base.full_name
    , base.address_country
    , base.highest_education
    , base.gender
    , base.birth_year
    , base.company
    , base.job_title
    , base.industry
    , learn_user_topic_interests.topic_interests as topic_interests
    , learn_profile.user_goals as goals
    , learn_profile.user_delivery_preference as delivery_preference
    , learn_profile.user_completed_onboarding as completed_onboarding
    , learn_profile.user_certificate_desired as certificate_desired
    , agg.user_is_active_on_mitlearn
    , agg.user_joined_on_mitlearn
    , agg.user_is_active_on_mitxonline
    , agg.user_joined_on_mitxonline
    , agg.user_is_active_on_edxorg
    , agg.user_joined_on_edxorg
    , agg.user_is_active_on_mitxpro
    , agg.user_joined_on_mitxpro
    , agg.user_is_active_on_residential
    , agg.user_joined_on_residential
from base_info as base
inner join agg_view as agg on base.user_pk = agg.user_pk
left join learn_profile on base.mitlearn_user_id = learn_profile.user_id
left join learn_user_topic_interests on learn_profile.profile_id = learn_user_topic_interests.profile_id
