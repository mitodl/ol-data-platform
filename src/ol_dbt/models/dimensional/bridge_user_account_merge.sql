{{ config(
    materialized='table'
) }}

-- Verified account-merge redirects derived from mitxonline audit history
-- (an admin reassigning a record's owner confirms two accounts are the same
-- person). Only mitxonline today; `platform` allows adding others later.
-- Grain: one row per (platform, platform_user_id_before). A merge can be
-- reversed later, so only the most recent direction per account pair is kept.
with courserunenrollment_audit as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_courserunenrollmentaudit') }}
)

, programenrollment_audit as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_programenrollmentaudit') }}
)

, user_changes as (
    select
        json_extract_scalar(enrollmentaudit_data_before, '$.user') as platform_user_id_before
        , json_extract_scalar(enrollmentaudit_data_after, '$.user') as platform_user_id_after
        , enrollmentaudit_created_on
    from courserunenrollment_audit
    where json_extract_scalar(enrollmentaudit_data_before, '$.user')
        != json_extract_scalar(enrollmentaudit_data_after, '$.user')

    union all

    select
        json_extract_scalar(enrollmentaudit_data_before, '$.user') as platform_user_id_before
        , json_extract_scalar(enrollmentaudit_data_after, '$.user') as platform_user_id_after
        , enrollmentaudit_created_on
    from programenrollment_audit
    where json_extract_scalar(enrollmentaudit_data_before, '$.user')
        != json_extract_scalar(enrollmentaudit_data_after, '$.user')
)

, grouped as (
    select
        cast(platform_user_id_before as integer) as platform_user_id_before
        , cast(platform_user_id_after as integer) as platform_user_id_after
        , max(enrollmentaudit_created_on) as last_merged_on
    from user_changes
    group by platform_user_id_before, platform_user_id_after
)

-- Normalize to an unordered pair so a reversed merge (both directions
-- present) competes against itself, keeping only the most recent direction.
-- Note: QUALIFY is not supported by Trino; using ROW_NUMBER subquery.
, ranked as (
    select
        *
        , row_number() over (
            partition by
                least(platform_user_id_before, platform_user_id_after)
                , greatest(platform_user_id_before, platform_user_id_after)
            order by last_merged_on desc
        ) as row_num
    from grouped
)

select
    'mitxonline' as platform
    , platform_user_id_before
    , platform_user_id_after
    , last_merged_on as merged_on
from ranked
where row_num = 1
