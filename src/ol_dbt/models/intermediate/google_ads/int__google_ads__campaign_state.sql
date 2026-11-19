-- ad_opt_v2's account-state snapshot, from each account's latest daily snapshot.
with courses as (
    select course, account_id from {{ ref('google_ads_courses') }}
)

, latest_snapshots as (
    select account_id, max(snapshot_date) as snapshot_date
    from {{ ref('stg__google_ads__api__campaign_snapshots') }}
    group by account_id
)

, campaigns as (
    select
        courses.course
        , campaign_snapshots.account_id
        , campaign_snapshots.snapshot_date
        , campaign_snapshots.campaign_resource_name
        , campaign_snapshots.campaign_id
        , trim(campaign_snapshots.campaign_name) as campaign_name
        , lower(campaign_snapshots.campaign_status) as campaign_status
        , campaign_snapshots.campaign_start_at
        , campaign_snapshots.campaign_end_at
        , campaign_snapshots.campaign_bidding_strategy_type
        , campaign_snapshots.campaign_budget_amount
        , campaign_snapshots.campaign_budget_reference_count > 1 as campaign_budget_is_shared
    from {{ ref('stg__google_ads__api__campaign_snapshots') }} as campaign_snapshots
    inner join latest_snapshots
        on
            campaign_snapshots.account_id = latest_snapshots.account_id
            and campaign_snapshots.snapshot_date = latest_snapshots.snapshot_date
    inner join courses on campaign_snapshots.account_id = courses.account_id
    where
        {{ google_ads_is_search_campaign('campaign_snapshots.campaign_name', 'campaign_snapshots.campaign_channel_type') }}
        and campaign_snapshots.campaign_status != 'REMOVED'
)

-- Counts only enabled keywords in enabled ad groups, as ad_opt_v2's state intervals do.
, keyword_counts as (
    select
        keywords.account_id
        , keywords.campaign_id
        , sum(case when not keywords.keyword_is_negative and keywords.keyword_match_type = 'BROAD' then 1 else 0 end) as broad_keyword_count
        , sum(case when not keywords.keyword_is_negative and keywords.keyword_match_type = 'PHRASE' then 1 else 0 end) as phrase_keyword_count
        , sum(case when not keywords.keyword_is_negative and keywords.keyword_match_type = 'EXACT' then 1 else 0 end) as exact_keyword_count
        , count(distinct case when not keywords.keyword_is_negative then lower(keywords.keyword_text) end)
        as unique_keyword_count
        , sum(case when keywords.keyword_is_negative then 1 else 0 end) as ad_group_negative_keyword_count
    from {{ ref('stg__google_ads__api__keyword_snapshots') }} as keywords
    inner join latest_snapshots
        on
            keywords.account_id = latest_snapshots.account_id
            and keywords.snapshot_date = latest_snapshots.snapshot_date
    where keywords.keyword_status = 'ENABLED' and keywords.ad_group_status = 'ENABLED'
    group by keywords.account_id, keywords.campaign_id
)

, campaign_criterion_counts as (
    select
        criteria.account_id
        , criteria.campaign_id
        , sum(case when criteria.campaign_criterion_type = 'KEYWORD' and criteria.campaign_criterion_is_negative then 1 else 0 end)
        as campaign_negative_keyword_count
        , sum(case when criteria.campaign_criterion_type = 'LOCATION' and not criteria.campaign_criterion_is_negative then 1 else 0 end)
        as targeted_location_count
        , sum(case when criteria.campaign_criterion_type = 'LOCATION' and criteria.campaign_criterion_is_negative then 1 else 0 end)
        as excluded_location_count
    from {{ ref('stg__google_ads__api__campaign_criterion_snapshots') }} as criteria
    inner join latest_snapshots
        on
            criteria.account_id = latest_snapshots.account_id
            and criteria.snapshot_date = latest_snapshots.snapshot_date
    group by criteria.account_id, criteria.campaign_id
)

, shared_negative_counts as (
    select
        shared_sets.account_id
        , shared_sets.campaign_id
        , count(distinct shared_sets.shared_set_id) as shared_negative_list_count
        , count(shared_criteria.shared_criterion_resource_name) as shared_negative_keyword_count
    from {{ ref('stg__google_ads__api__campaign_shared_set_snapshots') }} as shared_sets
    inner join latest_snapshots
        on
            shared_sets.account_id = latest_snapshots.account_id
            and shared_sets.snapshot_date = latest_snapshots.snapshot_date
    left join {{ ref('stg__google_ads__api__shared_criterion_snapshots') }} as shared_criteria
        on
            shared_sets.account_id = shared_criteria.account_id
            and shared_sets.snapshot_date = shared_criteria.snapshot_date
            and shared_sets.shared_set_id = shared_criteria.shared_set_id
    where
        shared_sets.campaign_shared_set_status = 'ENABLED'
        and shared_sets.shared_set_type = 'NEGATIVE_KEYWORDS'
    group by shared_sets.account_id, shared_sets.campaign_id
)

select
    campaigns.course
    , campaigns.snapshot_date
    , campaigns.campaign_id
    , campaigns.campaign_name
    , {{ google_ads_campaign_region('campaigns.campaign_name') }} as campaign_region
    , excluded_campaigns.campaign_name is not null as campaign_is_excluded
    , campaigns.campaign_status
    , campaigns.campaign_start_at
    , campaigns.campaign_end_at
    , campaigns.campaign_bidding_strategy_type
    , campaigns.campaign_budget_amount
    , campaigns.campaign_budget_is_shared
    , coalesce(keyword_counts.broad_keyword_count, 0) as broad_keyword_count
    , coalesce(keyword_counts.phrase_keyword_count, 0) as phrase_keyword_count
    , coalesce(keyword_counts.exact_keyword_count, 0) as exact_keyword_count
    , coalesce(keyword_counts.unique_keyword_count, 0) as unique_keyword_count
    , coalesce(keyword_counts.ad_group_negative_keyword_count, 0) as ad_group_negative_keyword_count
    , coalesce(campaign_criterion_counts.campaign_negative_keyword_count, 0) as campaign_negative_keyword_count
    , coalesce(shared_negative_counts.shared_negative_list_count, 0) as shared_negative_list_count
    , coalesce(shared_negative_counts.shared_negative_keyword_count, 0) as shared_negative_keyword_count
    , coalesce(campaign_criterion_counts.targeted_location_count, 0) as targeted_location_count
    , coalesce(campaign_criterion_counts.excluded_location_count, 0) as excluded_location_count
from campaigns
left join keyword_counts
    on campaigns.account_id = keyword_counts.account_id and campaigns.campaign_id = keyword_counts.campaign_id
left join campaign_criterion_counts
    on
        campaigns.account_id = campaign_criterion_counts.account_id
        and campaigns.campaign_id = campaign_criterion_counts.campaign_id
left join shared_negative_counts
    on
        campaigns.account_id = shared_negative_counts.account_id
        and campaigns.campaign_id = shared_negative_counts.campaign_id
left join ({{ google_ads_excluded_campaigns('campaigns') }}) as excluded_campaigns
    on campaigns.course = excluded_campaigns.course and campaigns.campaign_name = excluded_campaigns.campaign_name
