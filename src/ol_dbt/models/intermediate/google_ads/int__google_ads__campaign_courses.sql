-- Each campaign's course: the google_ads_campaign_courses row naming its campaign_id, else the
-- row whose pattern its latest name matches. A campaign that matches no row is not here, and the
-- other models keep its rows with no course. product_readable_id and product_name are for
-- people and joins; course is the key.
with campaigns as (
    select account_id, campaign_id, latest_name as campaign_name
    from {{ ref('int__google_ads__campaign_names') }}
    where name = latest_name
)

, rules as (
    select course, campaign_name_pattern, campaign_id, product_readable_id
    from {{ ref('google_ads_campaign_courses') }}
)

, by_id as (
    select campaigns.account_id, campaigns.campaign_id, rules.course, rules.product_readable_id
    from campaigns
    inner join rules on campaigns.campaign_id = rules.campaign_id
)

, by_name as (
    select campaigns.account_id, campaigns.campaign_id, rules.course, rules.product_readable_id
    from campaigns
    inner join rules on rules.campaign_id is null and campaigns.campaign_name like rules.campaign_name_pattern
    left join by_id on campaigns.account_id = by_id.account_id and campaigns.campaign_id = by_id.campaign_id
    where by_id.campaign_id is null
)

, product_names as (
    select course_readable_id as product_readable_id, max(course_title) as product_name
    from {{ ref('dim_course') }}
    group by course_readable_id
    union all
    select program_readable_id as product_readable_id, max(program_title) as product_name
    from {{ ref('dim_program') }}
    where program_readable_id is not null
    group by program_readable_id
)

select
    matched.account_id
    , matched.campaign_id
    , campaigns.campaign_name
    , matched.course
    , matched.product_readable_id
    , product_names.product_name
from (
    select account_id, campaign_id, course, product_readable_id from by_id
    union all
    select account_id, campaign_id, course, product_readable_id from by_name
) as matched
inner join campaigns on matched.account_id = campaigns.account_id and matched.campaign_id = campaigns.campaign_id
left join product_names on matched.product_readable_id = product_names.product_readable_id
