-- Each campaign's latest name, and every earlier name it had in a rename event. ad_opt_v2 pulls
-- its panels with current names and maps older names to them (parse_change_history's current_names).
with fetched_names as (
    select account_id, campaign_id, campaign_name, campaign_snapshot_retrieved_at as fetched_at
    from {{ ref('stg__google_ads__api__campaign_snapshots') }}
    union all
    -- A row keeps the name it had when fetched, and campaigns removed before the snapshots
    -- began appear only here.
    select account_id, campaign_id, campaign_name, campaign_daily_retrieved_at as fetched_at
    from {{ ref('stg__google_ads__api__campaign_daily') }}
)

, latest_names as (
    select account_id, campaign_id, trim(campaign_name) as campaign_name
    from (
        select
            account_id
            , campaign_id
            , campaign_name
            , row_number() over (partition by account_id, campaign_id order by fetched_at desc) as name_rank
        from fetched_names
    ) as ranked
    where name_rank = 1
)

, renamed_from as (
    select
        account_id
        , cast(regexp_extract(campaign_resource_name, '[0-9]+$') as bigint) as campaign_id
        , trim({{ json_query_string('change_old_resource', "'$.campaign.name'") }}) as previous_name
    from {{ ref('stg__google_ads__api__change_events') }}
    where
        change_resource_type = 'CAMPAIGN'
        and concat(',', change_changed_fields, ',') like '%,name,%'
)

select account_id, campaign_id, campaign_name as name, campaign_name as latest_name
from latest_names

union

select renamed_from.account_id, renamed_from.campaign_id, renamed_from.previous_name as name, latest_names.campaign_name
from renamed_from
inner join latest_names
    on renamed_from.account_id = latest_names.account_id and renamed_from.campaign_id = latest_names.campaign_id
where renamed_from.previous_name is not null and renamed_from.previous_name != latest_names.campaign_name
