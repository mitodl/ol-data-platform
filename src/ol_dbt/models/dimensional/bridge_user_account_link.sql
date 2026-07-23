{{ config(
    materialized='table'
) }}

-- Edges between platform accounts, independent of dim_user's own identity-
-- collapsing (grouping by hashed email). from_platform = to_platform is a
-- merge redirect (old account -> canonical, confirmed via mitxonline audit
-- history); different platforms is a co-reference, not a redirect.
-- Grain: (from_platform, from_user_id, to_platform).
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
, ranked_merges as (
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

, account_merges as (
    select
        'mitxonline' as from_platform
        , platform_user_id_before as from_user_id
        , 'mitxonline' as to_platform
        , platform_user_id_after as to_user_id
        , last_merged_on as observed_on
    from ranked_merges
    where row_num = 1
)

, platform_links as (
    select
        'mitxonline' as from_platform
        , user_mitxonline_id as from_user_id
        , 'edxorg' as to_platform
        , user_edxorg_id as to_user_id
        , cast(null as varchar) as observed_on
    from {{ ref('int__mitx__users') }}
    where user_mitxonline_id is not null
        and user_edxorg_id is not null
)

select * from account_merges
union all
select * from platform_links
