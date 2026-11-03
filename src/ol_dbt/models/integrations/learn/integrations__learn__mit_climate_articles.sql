{#
  integrations__learn__mit_climate_articles
  Exposes MIT Climate Portal articles for MIT Learn, as its legacy ETL
  (learning_resources/etl/mit_climate.py) writes them. The delivery asset joins the
  summary, footnotes and byline into full_description.
  Contract: docs/learn_marts_contract.md

  STALENESS: the raw table is merge-loaded, so an article the feeds drop keeps the
  _dlt_load_id of the last load that saw it. Only rows from the most recent load are
  kept. The dlt source raises on a feed's HTTP error, so a failed feed fails the load,
  but a feed that answers with an empty list drops its articles, as it did in MIT
  Learn's legacy ETL.

  Topics resolve through int__learn__offeror_topics rather than the gated lookup,
  because this model builds in QA, which has no copy of MIT Learn's topic tables. So a
  failing assert_learn_topic_seeds_match_mitlearn does not stop this model.
#}

with loaded_articles as (
    select * from {{ ref('stg__mit_climate__api__articles') }}
)

, articles as (
    select
        *
        , split({{ html_unescape('article_topics_raw') }}, '|') as article_topic_names
    from loaded_articles
    where
        article_uuid is not null
        and article_dlt_load_id = (
            select max(article_loads.article_dlt_load_id) from loaded_articles as article_loads
        )
)

, topic_lookup as (
    select * from {{ ref('int__learn__offeror_topics') }}
    where offeror_code = 'climate'
)

, article_topics as (
    select
        articles.article_uuid
        , array_agg(distinct topic_lookup.topic_name order by topic_lookup.topic_name) as topics
    from articles
    cross join unnest(articles.article_topic_names) as article_topic (topic_name)
    inner join topic_lookup on trim(article_topic.topic_name) = topic_lookup.offeror_topic_name
    group by articles.article_uuid
)

select
    articles.article_uuid                                   as readable_id
    , {{ html_unescape('articles.article_title') }}         as title
    -- MIT Climate gives a date, which MIT Learn reads as midnight US Eastern
    , {{ format_timestamp_as_iso8601(timestamptz_at_utc(
        'coalesce(' ~ local_date_to_timestamptz("nullif(articles.article_created_on_raw, '')", 'America/New_York')
        ~ ', ' ~ current_timestamptz() ~ ')'
    ), fractional_digits=3) }}                              as last_modified
    , 'mit_climate'                                         as etl_source
    , articles.article_summary                              as description
    -- MIT Climate API returns relative paths; prepend the canonical base URL
    , concat('https://climate.mit.edu', articles.article_url) as url
    , case
        when articles.article_image_src is not null and articles.article_image_src != ''
            then concat('https://climate.mit.edu', articles.article_image_src)
    end                                                     as image_url
    , articles.article_image_alt                            as image_alt
    , article_topics.topics
    , articles.article_footnotes                            as footnotes
    , articles.article_byline                               as byline
    , articles.article_feed_type                            as feed_type
    , true                                                  as published
    , 'climate'                                             as platform
from articles
left join article_topics on articles.article_uuid = article_topics.article_uuid
