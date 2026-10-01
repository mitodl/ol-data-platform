{#
  integrations__learn__news_feed_items
  One row per news item MIT Learn's news_events app loads from an external feed,
  with its feed source's fields repeated on the row: the MIT Open Learning Medium
  publication (news_events/etl/medium_mit_news.py) and MIT Professional Education
  news (mitpe_news.py). Learn's own website content news is not here: it is Learn's
  data and its sync is event-driven inside Learn.

  summary and content are the feed's HTML, unsanitized. Learn runs both through
  nh3 (main.utils.clean_data), the Medium summary with every tag stripped, and
  the receiver is where that has to happen.
#}

with medium_posts as (
    select * from {{ ref('stg__medium__rss__posts') }}
)

, mitpe_news as (
    select * from {{ ref('stg__mitpe__api__news') }}
)

, medium_items as (
    select
        feed_title as feed_source_title
        , feed_url as feed_source_url
        , feed_description as feed_source_description
        , feed_image_url as feed_source_image_url
        , feed_image_title as feed_source_image_alt
        , feed_image_title as feed_source_image_description
        , post_guid as guid
        , post_title as title
        , post_url as url
        -- feedparser reads content:encoded as the summary when an item has no
        -- description, which Medium's never do.
        , {{ regexp_replace_all('coalesce(post_description, post_content)', "'<[^>]*>'", "''") }} as summary
        , post_content as content
        -- Learn's image is the <img> that opens the first <figure>, if one does:
        -- the leftmost "<figure>" match takes the <img> only when it follows directly.
        , {{ regexp_extract_or_null(
            regexp_extract_or_null('post_content', "'<figure>(<img[^>]*>)?'"), "'<img[^>]*>'"
        ) }} as figure_img
        , {{ json_extract_varchar_array('post_creators_json', "'$'") }} as authors
        , array_sort({{ json_extract_varchar_array('post_categories_json', "'$'") }}) as topics
        , {{ format_timestamp_as_iso8601(
            local_timestamp_to_timestamptz(
                date_parse('post_published_on_raw', "'%a, %d %b %Y %H:%i:%s GMT'"), "'UTC'"
            )
        ) }} as publish_date
    from medium_posts
)

, mitpe_items as (
    select
        'MIT Professional Education News' as feed_source_title
        , {{ url_join("'" ~ var("mitpe_url") ~ "'", "'/news'") }} as feed_source_url
        , '
News and updates from MIT Professional Education.
' as feed_source_description
        , cast(null as varchar) as feed_source_image_url
        , cast(null as varchar) as feed_source_image_alt
        , cast(null as varchar) as feed_source_image_description
        , news_id as guid
        , {{ html_unescape('news_title') }} as title
        , {{ url_join("'" ~ var("mitpe_url") ~ "'", 'news_url') }} as url
        , {{ html_unescape('news_summary') }} as summary
        , {{ html_unescape('news_summary') }} as content
        , case
            when news_image_src is not null and news_image_src != ''
                then {{ url_join("'" ~ var("mitpe_url") ~ "'", 'news_image_src') }}
        end as image_url
        , case when news_image_src is not null and news_image_src != '' then {{ html_unescape('news_title') }} end
            as image_alt
        -- Learn's parse_authors splits on the first of "and" or "|" the string
        -- contains anywhere, including inside a name, and strips each part.
        , case
            when trim(coalesce(news_author_raw, '')) = '' then {{ empty_varchar_array() }}
            when strpos(news_author_raw, 'and') > 0
                then {{ array_filter_nonempty(regexp_split('trim(news_author_raw)', "'\\s*and\\s*'")) }}
            when strpos(news_author_raw, '|') > 0
                then {{ array_filter_nonempty(regexp_split('trim(news_author_raw)', "'\\s*\\|\\s*'")) }}
            else array[trim(news_author_raw)]
        end as authors
        , {{ empty_varchar_array() }} as topics
        , {{ format_timestamp_as_iso8601(
            local_date_to_timestamptz("nullif(news_date_raw, '')", 'America/New_York')
        ) }} as publish_date
    from mitpe_news
)

select
    feed_source_title
    , feed_source_url
    , feed_source_description
    , feed_source_image_url
    , feed_source_image_alt
    , feed_source_image_description
    , 'news' as feed_type
    , guid
    , title
    , url
    , summary
    , content
    , {{ regexp_extract_or_null('figure_img', "'src=\"([^\"]*)\"'", 1) }} as image_url
    , {{ regexp_extract_or_null('figure_img', "'alt=\"([^\"]*)\"'", 1) }} as image_alt
    , coalesce({{ regexp_extract_or_null('figure_img', "'alt=\"([^\"]*)\"'", 1) }}, '') as image_description
    , authors
    , topics
    , publish_date
from medium_items

union all

select
    feed_source_title
    , feed_source_url
    , feed_source_description
    , feed_source_image_url
    , feed_source_image_alt
    , feed_source_image_description
    , 'news' as feed_type
    , guid
    , title
    , url
    , summary
    , content
    , image_url
    , image_alt
    , image_alt as image_description
    , authors
    , topics
    , publish_date
from mitpe_items
