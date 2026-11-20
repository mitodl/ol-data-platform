-- Program and Course Search campaigns with clicks in the last 30 days that no
-- google_ads_campaign_courses row maps, so the optimizer leaves them out: usually a new
-- course code that needs a row.
{{ config(severity='warn') }}

select names.account_id, names.campaign_id
from {{ ref('int__google_ads__campaign_names') }} as names
inner join {{ ref('stg__google_ads__api__campaign_daily') }} as campaign_daily
    on names.account_id = campaign_daily.account_id and names.campaign_id = campaign_daily.campaign_id
left join {{ ref('int__google_ads__campaign_courses') }} as courses
    on names.account_id = courses.account_id and names.campaign_id = courses.campaign_id
where
    names.name = names.latest_name
    and (names.latest_name like 'Program - %' or names.latest_name like 'Course - %')
    and campaign_daily.campaign_channel_type = 'SEARCH'
    and campaign_daily.report_date >= {{ dbt.dateadd('day', -30, 'current_date') }}
    and courses.campaign_id is null
group by names.account_id, names.campaign_id
having sum(campaign_daily.clicks) > 0
