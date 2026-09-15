{#-
  The term a `search_update` event records is not reliably the term the user
  just submitted. PostHog derives `search_param_q` from `$current_url`, and MIT
  Learn captures the event before the Next.js router commits the navigation, so
  on events captured before the frontend fix ships the URL still holds the
  PREVIOUS query. `search_term` therefore prefers an explicit `search_term`
  property carried on the event and falls back to the URL-derived value only
  when the event does not carry one; `search_term_is_from_event` says which
  of the two a row got.

  The `search_term` property is added by mitodl/mit-learn#3908. Until that
  ships nothing in the lake carries it, so every row falls back to the URL.
-#}

with source as (

    select *
    from {{ source('ol_warehouse_raw_data', 'raw__posthog__learn__s3__events') }}
    where event = 'search_update'

)

-- The raw table is at-least-once at hour granularity: a run that fails after
-- committing part of a load package re-reads those hours on the next attempt.
-- Without this the documented one-row-per-event grain does not hold and search
-- counts double for the replayed hours.
, source_deduped as (

    select
        *
        , row_number() over (
            partition by uuid order by _inserted_at desc, s3_object_key desc
        ) as row_num
    from source

)

, extracted as (

    select
        uuid as search_event_uuid
        , timestamp as search_event_timestamp
        , distinct_id as search_user_distinct_id
        , person_id as search_user_person_id
        , s3_object_key as search_event_source_object_key
        , nullif({{ json_query_string('properties', '\'$."$current_url"\'') }}, '')
            as search_page_url
        , nullif({{ json_query_string('properties', "'$.search_term'") }}, '')
            as search_term_from_event
        , nullif({{ json_query_string('properties', "'$.search_param_q'") }}, '')
            as search_term_from_url
        , cast(
            nullif({{ json_query_string('properties', "'$.isEnter'") }}, '') as boolean
        ) as search_submitted_with_enter_key
    from source_deduped
    where row_num = 1

)

select
    search_event_uuid
    , search_event_timestamp
    , search_user_distinct_id
    , search_user_person_id
    , search_event_source_object_key
    , search_page_url
    -- Path only: everything from the host up to the query string or fragment.
    -- `url_extract_path` is Trino-only and this model also compiles on DuckDB.
    , nullif(regexp_extract(search_page_url, '^https?://[^/]+([^?#]*)', 1), '')
        as search_page_path
    , search_term_from_event
    , search_term_from_url
    , coalesce(search_term_from_event, search_term_from_url) as search_term
    , search_term_from_event is not null as search_term_is_from_event
    , search_submitted_with_enter_key
from extracted
