-- ad_opt_v2's compute_segment_conv_per_click_rates over the whole campaign-day panel: conversions
-- per click for each region and match-type set. A day with no region is left out, as pandas'
-- groupby drops it.
select
    product_readable_id
    , campaign_region
    , match_types
    , sum(clicks) as segment_clicks
    , sum(all_conversions) as segment_conversions
    , case when sum(clicks) > 0 then sum(all_conversions) / sum(clicks) end as conversions_per_click
from {{ ref('int__google_ads__campaign_day_panel') }}
where campaign_region is not null
group by product_readable_id, campaign_region, match_types
