{#
  integrations__learn__event_feed_items
  One row per upcoming event MIT Learn's news_events app loads, with its feed
  source's fields repeated on the row: openlearning.mit.edu events
  (news_events/etl/ol_events.py) and MIT Professional Education events
  (mitpe_events.py).

  Upcoming means starting at or after the build. Learn's loader deletes every
  event whose start has passed on each load, whatever its end, so that is what
  Learn holds. A daily build therefore carries events that started earlier the
  same day; the receiver should drop a past start the same way.

  summary and content are the feed's HTML, unsanitized; Learn runs both through
  nh3 (main.utils.clean_data), and the receiver is where that has to happen.
#}

with ol_events as (
    select * from {{ ref('stg__openlearning__api__events') }}
)

, mitpe_events as (
    select * from {{ ref('stg__mitpe__api__events') }}
)

, mitpe_event_times as (
    select * from {{ ref('int__news_events__mitpe_event_times') }}
)

, ol_items as (
    select
        'Open Learning Events' as feed_source_title
        , 'https://openlearning.mit.edu/events/' as feed_source_url
        -- Learn's OL_EVENTS_DESCRIPTION constant, typo included.
        , '
Open Learning hosts a wide range of events for learners, educators, researchers,
nd practitioners around the world.
Regular event series include Open Learning Talks, which bring together leaders
to discuss research-based ideas, technologies, and efforts in education; and xTalks,
which provide MIT faculty, researchers, staff, and students an opportunity to share
their experiences developing and using digital technologies in the classroom.
Explore upcoming events below, including webinars, workshops, and more.
' as feed_source_description
        , event_id as guid
        , event_title as title
        , case
            when event_path_alias is not null and event_path_alias != ''
                then {{ url_join("'https://openlearning.mit.edu'", 'event_path_alias') }}
        end as url
        , coalesce(event_body, '') as summary
        , coalesce(event_body, '') as content
        , case
            when event_image_src is not null
                then {{ url_join("'https://openlearning.mit.edu'", 'event_image_src') }}
        end as image_url
        , event_image_alt as image_alt
        , event_image_title as image_description
        , {{ json_extract_varchar_array('event_audience_json', "'$'") }} as audience
        , {{ json_extract_varchar_array('event_location_json', "'$'") }} as location
        , {{ json_extract_varchar_array('event_category_json', "'$'") }} as event_type
        -- Learn replaces the date's own UTC offset with US/Eastern, keeping the
        -- wall-clock time.
        , {{ local_timestamp_to_timestamptz(
            "replace(substr(event_start_on_raw, 1, 19), 'T', ' ')", "'America/New_York'"
        ) }} as event_start_on
        -- The site gives an end time, which Learn does not load.
        , {{ null_timestamptz() }} as event_end_on
    from ol_events
    where event_is_published and event_start_on_raw is not null
)

, mitpe_items as (
    select
        'MIT Professional Education Events' as feed_source_title
        , {{ url_join("'" ~ var("mitpe_url") ~ "'", "'/events'") }} as feed_source_url
        , '
MIT Professional Education events.
' as feed_source_description
        , mitpe_events.event_id as guid
        , {{ html_unescape('mitpe_events.event_title') }} as title
        , {{ url_join("'" ~ var("mitpe_url") ~ "'", 'mitpe_events.event_url') }} as url
        , {{ html_unescape('mitpe_events.event_summary') }} as summary
        , {{ html_unescape('mitpe_events.event_summary') }} as content
        , case
            when mitpe_events.event_image_src is not null and mitpe_events.event_image_src != ''
                then {{ url_join("'" ~ var("mitpe_url") ~ "'", 'mitpe_events.event_image_src') }}
        end as image_url
        -- Unlike the title, the image text is the feed's title as sent.
        , case
            when mitpe_events.event_image_src is not null and mitpe_events.event_image_src != ''
                then mitpe_events.event_title
        end as image_alt
        , case
            when mitpe_events.event_image_src is not null and mitpe_events.event_image_src != ''
                then mitpe_events.event_title
        end as image_description
        , {{ array_of(["'Faculty'", "'MIT Community'", "'Public'", "'Students'"]) }} as audience
        , {{ empty_varchar_array() }} as location
        , {{ empty_varchar_array() }} as event_type
        , mitpe_event_times.start_on as event_start_on
        , mitpe_event_times.end_on as event_end_on
    from mitpe_events
    inner join mitpe_event_times on mitpe_events.event_id = mitpe_event_times.event_id
)

, items as (
    select * from ol_items
    union all
    select * from mitpe_items
)

select
    feed_source_title
    , feed_source_url
    , feed_source_description
    , cast(null as varchar) as feed_source_image_url
    , cast(null as varchar) as feed_source_image_alt
    , cast(null as varchar) as feed_source_image_description
    , 'events' as feed_type
    , guid
    , title
    , url
    , summary
    , content
    , image_url
    , image_alt
    , image_description
    , audience
    , location
    , event_type
    , {{ format_timestamp_as_iso8601(timestamptz_at_utc('event_start_on')) }} as event_datetime
    , {{ format_timestamp_as_iso8601(timestamptz_at_utc('event_end_on')) }} as event_end_datetime
from items
where event_start_on >= {{ current_timestamptz() }}
