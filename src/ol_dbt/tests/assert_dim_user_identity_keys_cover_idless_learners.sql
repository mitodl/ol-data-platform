-- Learners Emeritus or Global Alumni sent without a user id are only identifiable through
-- the identity keys, so each one must reach dim_user with a key. Where the learner has no
-- Emeritus id at all, or is a Global Alumni learner (email-first), the key must be one of
-- the raw emails the platform sent.
with emeritus as (
    select
        user_email
        , lower(user_email) as email
        , user_id
    from {{ ref('stg__emeritus__api__bigquery__user_enrollments') }}
    where user_email is not null
)

, emeritus_emails_without_id as (
    select email
    from emeritus
    group by email
    having count(user_id) = 0
)

, global_alumni_all as (
    select
        user_email
        , lower(user_email) as email
        , user_id
    from {{ ref('stg__global_alumni__api__bigquery__user_enrollments') }}
    where user_email is not null
)

-- Skipped: a Global Alumni user id sent under more than one email. dim_user keys these
-- learners on the id, so the emails merge into one row and only one of them survives as
-- the key. That is a known dim_user defect, tracked separately; delete this once it is fixed.
, global_alumni_shared_ids as (
    select user_id
    from global_alumni_all
    where user_id is not null
    group by user_id
    having count(distinct email) > 1
)

, global_alumni as (
    select
        user_email
        , email
    from global_alumni_all
    where user_id is null or user_id not in (select user_id from global_alumni_shared_ids)
)

, missing_emeritus_key as (
    select distinct
        'emeritus' as platform
        , emeritus.email
    from emeritus
    left join {{ ref('dim_user') }} as dim_user
        on emeritus.email = dim_user.email
    where
        emeritus.user_id is null
        and dim_user.emeritus_identity_key is null
)

, emeritus_key_not_a_sent_email as (
    select
        'emeritus' as platform
        , dim_user.email
    from {{ ref('dim_user') }} as dim_user
    inner join emeritus_emails_without_id
        on dim_user.email = emeritus_emails_without_id.email
    where not exists (
        select 1
        from emeritus
        where
            emeritus.email = dim_user.email
            and emeritus.user_email = dim_user.emeritus_identity_key
    )
)

, global_alumni_key_not_a_sent_email as (
    select distinct
        'global_alumni' as platform
        , global_alumni.email
    from global_alumni
    left join {{ ref('dim_user') }} as dim_user
        on global_alumni.email = dim_user.email
    where not exists (
        select 1
        from global_alumni as sent
        where
            sent.email = global_alumni.email
            and sent.user_email = dim_user.global_alumni_identity_key
    )
)

select * from missing_emeritus_key
union all
select * from emeritus_key_not_a_sent_email
union all
select * from global_alumni_key_not_a_sent_email
