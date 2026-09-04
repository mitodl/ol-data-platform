{#-
  The term a `search_update` event records is not reliably the term the user
  just submitted. PostHog derives `search_param_q` from `$current_url`, and MIT
  Learn captures the event before the Next.js router commits the navigation, so
  on events captured before the frontend fix ships the URL still holds the
  PREVIOUS query. `search_query` therefore prefers an explicit `search_query`
  property carried on the event and falls back to the URL-derived value only
  when the event does not carry one; `search_query_is_from_event` says which
  of the two a row got.

  The `search_query` property name is a forward contract with the pending
  mitodl/mit-learn fix and is not emitted by any event in the lake yet. Reconcile
  it against that PR when it lands rather than trusting it on sight.
-#}

with source as (

    select *
    from {{ source('ol_warehouse_raw_data', 'raw__posthog__learn__s3__events') }}
    where event = 'search_update'

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
        , nullif({{ json_query_string('properties', "'$.search_query'") }}, '')
            as search_query_from_event
        , nullif({{ json_query_string('properties', "'$.search_param_q'") }}, '')
            as search_query_from_url
        , cast(
            nullif({{ json_query_string('properties', "'$.isEnter'") }}, '') as boolean
        ) as search_submitted_with_enter_key
    from source

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
    , search_query_from_event
    , search_query_from_url
    , coalesce(search_query_from_event, search_query_from_url) as search_query
    , search_query_from_event is not null as search_query_is_from_event
    , search_submitted_with_enter_key
from extracted
