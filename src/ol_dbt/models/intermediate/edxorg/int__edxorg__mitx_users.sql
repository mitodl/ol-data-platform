---Intermediate MITx users from edx.org
---User data mostly comes from user_info_combo but email or username could be blank in user_info_combo for some older
-- courses. For these cases, we use email or username from email_opt_in.

with user_info_combo as (
    select *
    from {{ ref('stg__edxorg__bigquery__mitx_user_info_combo') }}
    where courserun_platform = '{{ var("edxorg") }}'
)

--- email_opt_in is one row per course_id with the same user_* data, use window function to pick one per user_id
, email_opt_in as (
    select
        user_id
        , user_email
        , user_username
        , user_full_name
        , row_number() over (partition by user_id order by user_email_opt_in_updated_on desc) as row_num
    from {{ ref('stg__edxorg__bigquery__mitx_user_email_opt_in') }}
    where courserun_platform = '{{ var("edxorg") }}'
)

, most_recent_user_info as (
    select
        user_id
        , user_email
        , user_username
        , user_full_name
    from email_opt_in
    where row_num = 1
)

, combined_user_info as (
    select
        user_info_combo.user_id
        , user_info_combo.user_full_name
        , user_info_combo.user_country
        , user_info_combo.user_city
        , user_info_combo.user_birth_year
        , user_info_combo.user_mailing_address
        , user_info_combo.user_highest_education
        , user_info_combo.user_profile_goals
        , user_info_combo.user_profile_meta
        , user_info_combo.user_joined_on
        , user_info_combo.user_gender
        , user_info_combo.user_last_login
        , coalesce(user_info_combo.user_email, most_recent_user_info.user_email) as user_email
        , coalesce(user_info_combo.user_username, most_recent_user_info.user_username) as user_username
        , row_number()
            over (partition by user_info_combo.user_id order by user_info_combo.user_last_login desc
            )
        as row_num
    from user_info_combo
    left join most_recent_user_info
        on user_info_combo.user_id = most_recent_user_info.user_id
)

, users as (
    select
        user_id
        , user_email
        , user_username
        , user_full_name
        , user_country
        , user_city
        , user_mailing_address
        , user_highest_education
        , user_birth_year
        , user_profile_goals
        , user_profile_meta
        , user_joined_on
        , user_gender
        , user_last_login
        , if(user_email like 'retired__user%' or user_username like 'retired__user%', false, true) as user_is_active
    from combined_user_info
    where row_num = 1
)

select * from users
