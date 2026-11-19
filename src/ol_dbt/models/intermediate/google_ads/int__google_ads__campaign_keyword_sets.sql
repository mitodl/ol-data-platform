-- ad_opt_v2's campaign-keyword-sets.csv: one row per distinct set of keyword lists.
select
    course
    , keyword_set_id
    , max(broad_keywords) as broad_keywords
    , max(phrase_keywords) as phrase_keywords
    , max(exact_keywords) as exact_keywords
    , max(unique_keywords) as unique_keywords
    , max(negative_broad_keywords) as negative_broad_keywords
    , max(negative_phrase_keywords) as negative_phrase_keywords
    , max(negative_exact_keywords) as negative_exact_keywords
    , max(negative_keywords) as negative_keywords
    , {{ array_join(
        array_agg_filter('cast(campaign_version as varchar)', 'true', order_by='campaign_version')
        , '; '
    ) }} as used_by_campaign_versions
from {{ ref('int__google_ads__campaign_versions') }}
group by course, keyword_set_id
