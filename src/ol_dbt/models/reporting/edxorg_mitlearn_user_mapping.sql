-- Referencing bridge_user_account_link (not just dim_user) is intentional:
-- dim_user can fold unrelated accounts together when their computed emails
-- collide, and its own output is the thing being validated, so it can't be
-- used to catch its own collisions -- the bridge has the pre-collapse link.
with mitxonline_edxorg_link as (
    select from_user_id as mitxonline_application_user_id, to_user_id as user_edxorg_id
    from {{ ref('bridge_user_account_link') }}
    where from_platform = 'mitxonline' and to_platform = 'edxorg'
)

, users as (
    select
        dim_user.mitxonline_application_user_id
        , dim_user.mitxonline_openedx_user_id
        , case
            when mitxonline_edxorg_link.user_edxorg_id = dim_user.edxorg_openedx_user_id
                then dim_user.edxorg_openedx_user_id
        end as edxorg_user_id
        , dim_user.mitlearn_user_id as mitlearn_application_user_id
    from {{ ref('dim_user') }} as dim_user
    left join mitxonline_edxorg_link
        on dim_user.mitxonline_application_user_id = mitxonline_edxorg_link.mitxonline_application_user_id
    where dim_user.mitxonline_application_user_id is not null
)

, merges as (
    select from_user_id as platform_user_id_before, to_user_id as platform_user_id_after
    from {{ ref('bridge_user_account_link') }}
    where from_platform = 'mitxonline' and to_platform = 'mitxonline'
)

-- A canonical account can have more than one account merged into it, so
-- aggregate per canonical account. max() only stands in for coalesce here --
-- the count(distinct ...) guard nulls the result out instead of guessing if
-- two merged-away accounts carry genuinely conflicting non-null values.
, merged_away_links as (
    select
        merges.platform_user_id_after as mitxonline_user_id
        , case
            when count(distinct users.edxorg_user_id) <= 1
                then max(users.edxorg_user_id)
        end as edxorg_user_id
        , case
            when count(distinct users.mitlearn_application_user_id) <= 1
                then max(users.mitlearn_application_user_id)
        end as mitlearn_application_user_id
    from merges
    inner join users
        on merges.platform_user_id_before = users.mitxonline_application_user_id
    group by merges.platform_user_id_after
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
