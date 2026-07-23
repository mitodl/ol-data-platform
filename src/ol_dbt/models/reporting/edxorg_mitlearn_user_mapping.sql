-- Referencing int__mitx__users (not just dim_user) is intentional: dim_user
-- can fold unrelated accounts together when their computed emails collide,
-- and its own output is the thing being validated, so it can't be used to
-- catch its own collisions -- int__mitx__users has the pre-collapse link.
with mitx_users as (
    select user_mitxonline_id, user_edxorg_id
    from {{ ref('int__mitx__users') }}
)

, users as (
    select
        dim_user.mitxonline_application_user_id
        , dim_user.mitxonline_openedx_user_id
        , case
            when mitx_users.user_edxorg_id = dim_user.edxorg_openedx_user_id
                then dim_user.edxorg_openedx_user_id
        end as edxorg_user_id
        , dim_user.mitlearn_user_id as mitlearn_application_user_id
    from {{ ref('dim_user') }} as dim_user
    left join mitx_users
        on dim_user.mitxonline_application_user_id = mitx_users.user_mitxonline_id
    where dim_user.mitxonline_application_user_id is not null
)

, merges as (
    select platform_user_id_before, platform_user_id_after
    from {{ ref('bridge_user_account_merge') }}
    where platform = 'mitxonline'
)

, merged_away_links as (
    select
        merges.platform_user_id_after as mitxonline_user_id
        , users.edxorg_user_id
        , users.mitlearn_application_user_id
    from merges
    inner join users
        on merges.platform_user_id_before = users.mitxonline_application_user_id
)

, mapped as (
    select
        users.mitxonline_openedx_user_id
        , coalesce(users.edxorg_user_id, merged_away_links.edxorg_user_id) as edxorg_user_id
        , coalesce(users.mitlearn_application_user_id, merged_away_links.mitlearn_application_user_id)
            as mitlearn_application_user_id
    from users
    left join merged_away_links
        on users.mitxonline_application_user_id = merged_away_links.mitxonline_user_id
    where users.mitxonline_application_user_id not in (select platform_user_id_before from merges)
)

select mitxonline_openedx_user_id, edxorg_user_id, mitlearn_application_user_id
from mapped
where edxorg_user_id is not null or mitlearn_application_user_id is not null
