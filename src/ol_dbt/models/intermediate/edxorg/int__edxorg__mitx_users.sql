--- Intermediate MITx users for edx.org
--- user_info_combo contains user info for each course they enroll in, so need to aggregate it here
--  to get the latest user info per user_id

with user_info_combo as (
    --- use window function to generate row number based on user_last_login for each user
    select
        user_id
        , user_email
        , user_username
        , user_full_name
        , user_country
        , user_joined_on
        , row_number() over (partition by user_id order by user_last_login desc) as seq
    from {{ ref('stg__edxorg__bigquery__mitx_user_info_combo') }}
)

select
    user_id
    , user_email
    , user_username
    , user_full_name
    , user_country
    , user_joined_on
from user_info_combo
where seq = 1   -- use the latest row for each user
