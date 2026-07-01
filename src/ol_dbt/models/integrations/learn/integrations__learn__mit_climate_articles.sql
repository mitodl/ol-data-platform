{#
  integrations__learn__mit_climate_articles
  Exposes MIT Climate Portal articles for MIT Learn's ETL (Trino-pull or webhook).
  Contract: docs/learn_marts_contract.md
#}

with articles as (
    select * from {{ ref('stg__mit_climate__api__articles') }}
)

select
    article_uuid                                            as readable_id
    , article_title                                         as title
    , coalesce(
        {{ cast_timestamp_to_iso8601('article_created_on_raw') }},
        {{ cast_timestamp_to_iso8601('current_timestamp') }}
    )                                                       as last_modified
    , 'mit_climate'                                         as etl_source
    , article_summary                                       as description
    -- MIT Climate API returns relative paths; prepend the canonical base URL
    , concat('https://climate.mit.edu', article_url)       as url
    , case
        when article_image_src is not null and article_image_src != ''
            then concat('https://climate.mit.edu', article_image_src)
    end                                                     as image_url
    , article_image_alt                                     as image_alt
    -- Normalize pipe-separated topics to comma-separated per contract
    , replace(article_topics_raw, '|', ', ')               as topics
    , article_footnotes                                     as footnotes
    , article_byline                                        as byline
    , article_feed_type                                     as feed_type
    , true                                                  as published
    , 'climate'                                             as platform
from articles
where article_uuid is not null
