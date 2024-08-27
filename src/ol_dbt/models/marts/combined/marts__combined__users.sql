--- This model combines intermediate users from different platforms

with mitx__users as (
    select * from {{ ref('int__mitx__users') }}
)

, mitxpro_users as (
    select * from {{ ref('int__mitxpro__users') }}
)

, bootcamps_users as (
    select * from {{ ref('int__bootcamps__users') }}
)

, residential_users as (
    select * from {{ ref('int__mitxresidential__users') }}
)


, combined_enrollments as (
    select * from {{ ref('int__combined__courserun_enrollments') }}
)

, course_stats as (
    select
        user_email
        , count(distinct course_title) as num_of_course_enrolled
        , count(
            distinct
            case
                when courserungrade_is_passing = true then course_title
            end
        ) as num_of_course_passed
    from combined_enrollments
    group by user_email
)

, combined_users as (
    select
        user_mitxonline_id
        , user_edxorg_id
        , null as user_mitxpro_id
        , null as user_bootcamps_id
        , null as user_mitx_id
        , user_mitxonline_username
        , user_edxorg_username
        , null as user_mitxpro_username
        , null as user_bootcamps_username
        , null as user_mitx_username
        , case
            when user_is_active_on_mitxonline and user_joined_on_mitxonline > user_joined_on_edxorg
                then user_mitxonline_email
            else coalesce(user_edxorg_email, user_mitxonline_email, user_micromasters_email)
        end as user_email
        , case
            when user_is_active_on_mitxonline and user_joined_on_mitxonline > user_joined_on_edxorg
                then user_joined_on_mitxonline
            else coalesce(user_joined_on_edxorg, user_joined_on_mitxonline)
        end as user_joined_on
        , case
            when user_is_active_on_mitxonline and user_last_login_on_mitxonline > user_last_login_on_edxorg
                then user_last_login_on_mitxonline
            else coalesce(user_last_login_on_edxorg, user_last_login_on_mitxonline)
        end as user_last_login
        , case
            when user_is_active_on_mitxonline
                then user_is_active_on_mitxonline
            else user_is_active_on_edxorg
        end as user_is_active
        , case
            when is_mitxonline_user = true and is_edxorg_user = true
                then concat('{{ var("mitxonline") }}', ' and ', '{{ var("edxorg") }}')
            when is_mitxonline_user = true
                then '{{ var("mitxonline") }}'
            when is_edxorg_user = true
                then '{{ var("edxorg") }}'
        end as platforms
        , user_full_name
        , user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year
        , user_company
        , user_job_title
        , user_industry
    from mitx__users
    where is_mitxonline_user = true or is_edxorg_user = true

    union all

    select
        null as user_mitxonline_id
        , null as user_edxorg_id
        , user_id as user_mitxpro_id
        , null as user_bootcamps_id
        , null as user_mitx_id
        , null as user_mitxonline_username
        , null as user_edxorg_username
        , user_username as user_mitxpro_username
        , null as user_bootcamps_username
        , null as user_mitx_username
        , user_email
        , user_joined_on
        , user_last_login
        , user_is_active
        , '{{ var("mitxpro") }}' as platforms
        , user_full_name
        , user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year
        , user_company
        , user_job_title
        , user_industry
    from mitxpro_users

    union all

    select
        null as user_mitxonline_id
        , null as user_edxorg_id
        , null as user_mitxpro_id
        , user_id as user_bootcamps_id
        , null as user_mitx_id
        , null as user_mitxonline_username
        , null as user_edxorg_username
        , null as user_mitxpro_username
        , user_username as user_bootcamps_username
        , null as user_mitx_username
        , user_email
        , user_joined_on
        , user_last_login
        , user_is_active
        , '{{ var("bootcamps") }}' as platforms
        , user_full_name
        , user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year
        , user_company
        , user_job_title
        , user_industry
    from bootcamps_users

    union all

    select
        null as user_mitxonline_id
        , null as user_edxorg_id
        , null as user_mitxpro_id
        , null as user_bootcamps_id
        , user_id as user_mitx_id
        , null as user_mitxonline_username
        , null as user_edxorg_username
        , null as user_mitxpro_username
        , null as user_bootcamps_username
        , user_username as user_mitx_username
        , user_email
        , user_joined_on
        , user_last_login
        , user_is_active
        , '{{ var("residential") }}' as platforms
        , user_full_name
        , user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year
        , null as user_company
        , null as user_job_title
        , null as user_industry
    from residential_users
)

select
    combined_users.platforms
    , combined_users.user_email
    , combined_users.user_joined_on
    , combined_users.user_last_login
    , combined_users.user_is_active
    , combined_users.user_full_name
    , combined_users.user_address_country
    , combined_users.user_highest_education
    , combined_users.user_gender
    , combined_users.user_birth_year
    , combined_users.user_company
    , combined_users.user_job_title
    , combined_users.user_industry
    , combined_users.user_mitxonline_id
    , combined_users.user_edxorg_id
    , combined_users.user_mitxpro_id
    , combined_users.user_bootcamps_id
    , combined_users.user_mitx_id
    , combined_users.user_mitxonline_username
    , combined_users.user_edxorg_username
    , combined_users.user_mitxpro_username
    , combined_users.user_bootcamps_username
    , combined_users.user_mitx_username
    , course_stats.num_of_course_enrolled
    , course_stats.num_of_course_passed
from combined_users
left join course_stats on combined_users.user_email = course_stats.user_email
