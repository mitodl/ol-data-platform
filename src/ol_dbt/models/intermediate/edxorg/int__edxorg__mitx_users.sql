---Intermediate MITx users from edx.org
---user data is mostly in user_info_combo but email or username could be blank in user_info_combo in some older courses,
---to get the most recent user data, we combine user_info_combo and email_opt_in here to always use username
-- and email from email_opt_in and the rest profile fields from user_info_combo
-- Both tables are unique on user_id + course_id

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
        most_recent_user_info.user_id
        , most_recent_user_info.user_email
        , most_recent_user_info.user_username
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
        , row_number() over (partition by user_info_combo.user_id order by user_info_combo.user_last_login desc
        ) as row_num
    from user_info_combo
    inner join most_recent_user_info
        on
            user_info_combo.user_id = most_recent_user_info.user_id
            and user_info_combo.user_username = most_recent_user_info.user_username
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
    from combined_user_info
    where row_num = 1
)

select * from users
