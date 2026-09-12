{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    properties={
        "partitioning": "ARRAY['platform']",
    }
) }}

{% set problem_events =
    (
    'problem_check'
    , 'showanswer'
    )
%}

-- Precompute incremental watermarks once (1 scan instead of 8 correlated subqueries).
-- Each source CTE receives the pre-fetched max timestamp for its platform.
{% if is_incremental() %}
with watermarks as (
    select platform, max(event_timestamp) as max_ts
    from {{ this }}
    group by platform
)

-- data from tracking logs
, mitxonline_problem_events as (
    select
        user_username
        , openedx_user_id
        , courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , {{ json_query_string('useractivity_context_object', "'$.module.display_name'") }} as problem_name
        , {{ json_query_string('useractivity_event_object', "'$.problem_id'") }} as problem_block_id
        , {{ json_query_string('useractivity_event_object', "'$.answers'") }} as answers
        , {{ json_query_string('useractivity_event_object', "'$.attempts'") }} as attempt
        , {{ json_query_string('useractivity_event_object', "'$.success'") }} as success
        , {{ json_query_string('useractivity_event_object', "'$.grade'") }} as grade
        , {{ json_query_string('useractivity_event_object', "'$.max_grade'") }} as max_grade
        , {{ from_iso8601_timestamp_nanos('useractivity_timestamp') }} as event_timestamp
        , useractivity_timestamp as event_timestamp_iso8601
        , {{ iso8601_to_time_key('useractivity_timestamp') }} as time_fk
        , {{ iso8601_to_date_key('useractivity_timestamp') }} as date_fk
    from {{ ref('stg__mitxonline__openedx__tracking_logs__user_activity') }}
    left join watermarks on watermarks.platform = 'mitxonline'
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ problem_events }}
        and useractivity_event_source = 'server'
        and (
            watermarks.max_ts is null
            or {{ from_iso8601_timestamp_nanos('useractivity_timestamp') }} > watermarks.max_ts
        )
)

, xpro_problem_events as (
    select
        user_username
        , openedx_user_id
        , courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , {{ json_query_string('useractivity_context_object', "'$.module.display_name'") }} as problem_name
        , {{ json_query_string('useractivity_event_object', "'$.problem_id'") }} as problem_block_id
        , {{ json_query_string('useractivity_event_object', "'$.answers'") }} as answers
        , {{ json_query_string('useractivity_event_object', "'$.attempts'") }} as attempt
        , {{ json_query_string('useractivity_event_object', "'$.success'") }} as success
        , {{ json_query_string('useractivity_event_object', "'$.grade'") }} as grade
        , {{ json_query_string('useractivity_event_object', "'$.max_grade'") }} as max_grade
        , {{ from_iso8601_timestamp_nanos('useractivity_timestamp') }} as event_timestamp
        , useractivity_timestamp as event_timestamp_iso8601
        , {{ iso8601_to_time_key('useractivity_timestamp') }} as time_fk
        , {{ iso8601_to_date_key('useractivity_timestamp') }} as date_fk
    from {{ ref('stg__mitxpro__openedx__tracking_logs__user_activity') }}
    left join watermarks on watermarks.platform = 'mitxpro'
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ problem_events }}
        and useractivity_event_source = 'server'
        and (
            watermarks.max_ts is null
            or {{ from_iso8601_timestamp_nanos('useractivity_timestamp') }} > watermarks.max_ts
        )
)

, mitxresidential_problem_events as (
    select
        user_username
        , user_id
        , courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , {{ json_query_string('useractivity_context_object', "'$.module.display_name'") }} as problem_name
        , {{ json_query_string('useractivity_event_object', "'$.problem_id'") }} as problem_block_id
        , {{ json_query_string('useractivity_event_object', "'$.answers'") }} as answers
        , {{ json_query_string('useractivity_event_object', "'$.attempts'") }} as attempt
        , {{ json_query_string('useractivity_event_object', "'$.success'") }} as success
        , {{ json_query_string('useractivity_event_object', "'$.grade'") }} as grade
        , {{ json_query_string('useractivity_event_object', "'$.max_grade'") }} as max_grade
        , {{ from_iso8601_timestamp_nanos('useractivity_timestamp') }} as event_timestamp
        , useractivity_timestamp as event_timestamp_iso8601
        , {{ iso8601_to_time_key('useractivity_timestamp') }} as time_fk
        , {{ iso8601_to_date_key('useractivity_timestamp') }} as date_fk
    from {{ ref('stg__mitxresidential__openedx__tracking_logs__user_activity') }}
    left join watermarks on watermarks.platform = 'residential'
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ problem_events }}
        and useractivity_event_source = 'server'
        and (
            watermarks.max_ts is null
            or {{ from_iso8601_timestamp_nanos('useractivity_timestamp') }} > watermarks.max_ts
        )
)

, edxorg_problem_events as (
    select
        user_username
        , user_id
        , {{ format_course_id('courserun_readable_id') }} as courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , {{ json_query_string('useractivity_context_object', "'$.module.display_name'") }} as problem_name
        , {{ json_query_string('useractivity_event_object', "'$.problem_id'") }} as problem_block_id
        , {{ json_query_string('useractivity_event_object', "'$.answers'") }} as answers
        , {{ json_query_string('useractivity_event_object', "'$.attempts'") }} as attempt
        , {{ json_query_string('useractivity_event_object', "'$.success'") }} as success
        , {{ json_query_string('useractivity_event_object', "'$.grade'") }} as grade
        , {{ json_query_string('useractivity_event_object', "'$.max_grade'") }} as max_grade
        , {{ from_iso8601_timestamp_nanos('useractivity_timestamp') }} as event_timestamp
        , useractivity_timestamp as event_timestamp_iso8601
        , {{ iso8601_to_time_key('useractivity_timestamp') }} as time_fk
        , {{ iso8601_to_date_key('useractivity_timestamp') }} as date_fk
    from {{ ref('stg__edxorg__s3__tracking_logs__user_activity') }}
    left join watermarks on watermarks.platform = 'edxorg'
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ problem_events }}
        and useractivity_event_source = 'server'
        and (
            watermarks.max_ts is null
            or {{ from_iso8601_timestamp_nanos('useractivity_timestamp') }} > watermarks.max_ts
        )
)

{% else %}
-- Full-refresh path: no watermark filters
with mitxonline_problem_events as (
    select
        user_username
        , openedx_user_id
        , courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , {{ json_query_string('useractivity_context_object', "'$.module.display_name'") }} as problem_name
        , {{ json_query_string('useractivity_event_object', "'$.problem_id'") }} as problem_block_id
        , {{ json_query_string('useractivity_event_object', "'$.answers'") }} as answers
        , {{ json_query_string('useractivity_event_object', "'$.attempts'") }} as attempt
        , {{ json_query_string('useractivity_event_object', "'$.success'") }} as success
        , {{ json_query_string('useractivity_event_object', "'$.grade'") }} as grade
        , {{ json_query_string('useractivity_event_object', "'$.max_grade'") }} as max_grade
        , {{ from_iso8601_timestamp_nanos('useractivity_timestamp') }} as event_timestamp
        , useractivity_timestamp as event_timestamp_iso8601
        , {{ iso8601_to_time_key('useractivity_timestamp') }} as time_fk
        , {{ iso8601_to_date_key('useractivity_timestamp') }} as date_fk
    from {{ ref('stg__mitxonline__openedx__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ problem_events }}
        and useractivity_event_source = 'server'
)

, xpro_problem_events as (
    select
        user_username
        , openedx_user_id
        , courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , {{ json_query_string('useractivity_context_object', "'$.module.display_name'") }} as problem_name
        , {{ json_query_string('useractivity_event_object', "'$.problem_id'") }} as problem_block_id
        , {{ json_query_string('useractivity_event_object', "'$.answers'") }} as answers
        , {{ json_query_string('useractivity_event_object', "'$.attempts'") }} as attempt
        , {{ json_query_string('useractivity_event_object', "'$.success'") }} as success
        , {{ json_query_string('useractivity_event_object', "'$.grade'") }} as grade
        , {{ json_query_string('useractivity_event_object', "'$.max_grade'") }} as max_grade
        , {{ from_iso8601_timestamp_nanos('useractivity_timestamp') }} as event_timestamp
        , useractivity_timestamp as event_timestamp_iso8601
        , {{ iso8601_to_time_key('useractivity_timestamp') }} as time_fk
        , {{ iso8601_to_date_key('useractivity_timestamp') }} as date_fk
    from {{ ref('stg__mitxpro__openedx__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ problem_events }}
        and useractivity_event_source = 'server'
)

, mitxresidential_problem_events as (
    select
        user_username
        , user_id
        , courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , {{ json_query_string('useractivity_context_object', "'$.module.display_name'") }} as problem_name
        , {{ json_query_string('useractivity_event_object', "'$.problem_id'") }} as problem_block_id
        , {{ json_query_string('useractivity_event_object', "'$.answers'") }} as answers
        , {{ json_query_string('useractivity_event_object', "'$.attempts'") }} as attempt
        , {{ json_query_string('useractivity_event_object', "'$.success'") }} as success
        , {{ json_query_string('useractivity_event_object', "'$.grade'") }} as grade
        , {{ json_query_string('useractivity_event_object', "'$.max_grade'") }} as max_grade
        , {{ from_iso8601_timestamp_nanos('useractivity_timestamp') }} as event_timestamp
        , useractivity_timestamp as event_timestamp_iso8601
        , {{ iso8601_to_time_key('useractivity_timestamp') }} as time_fk
        , {{ iso8601_to_date_key('useractivity_timestamp') }} as date_fk
    from {{ ref('stg__mitxresidential__openedx__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ problem_events }}
        and useractivity_event_source = 'server'
)

, edxorg_problem_events as (
    select
        user_username
        , user_id
        , {{ format_course_id('courserun_readable_id') }} as courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , {{ json_query_string('useractivity_context_object', "'$.module.display_name'") }} as problem_name
        , {{ json_query_string('useractivity_event_object', "'$.problem_id'") }} as problem_block_id
        , {{ json_query_string('useractivity_event_object', "'$.answers'") }} as answers
        , {{ json_query_string('useractivity_event_object', "'$.attempts'") }} as attempt
        , {{ json_query_string('useractivity_event_object', "'$.success'") }} as success
        , {{ json_query_string('useractivity_event_object', "'$.grade'") }} as grade
        , {{ json_query_string('useractivity_event_object', "'$.max_grade'") }} as max_grade
        , {{ from_iso8601_timestamp_nanos('useractivity_timestamp') }} as event_timestamp
        , useractivity_timestamp as event_timestamp_iso8601
        , {{ iso8601_to_time_key('useractivity_timestamp') }} as time_fk
        , {{ iso8601_to_date_key('useractivity_timestamp') }} as date_fk
    from {{ ref('stg__edxorg__s3__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ problem_events }}
        and useractivity_event_source = 'server'
)

{% endif %}

, users as (
    select * from {{ ref('dim_user') }}
)

, platform as (
    select * from {{ ref('dim_platform') }}
)

{% if is_incremental() %}
-- Rows already in the target whose stored user_fk no longer matches what dim_user
-- resolves today. The watermarks above only re-select events newer than the last run,
-- so without this a dim_user re-key strands historical activity under the obsolete
-- key -- the same hazard tfact_grade and tfact_certificate guard against with their
-- stale_user_fk_lookup. delete+insert on event_id replaces the row in place.
-- One CTE per platform because the dim_user join keys differ per platform; each
-- prunes to one partition (the model is partitioned by platform) and joins by
-- equality rather than an OR across every platform's key pair.
-- Restricted to rows at or below the watermark so an event the source CTEs also
-- re-select cannot arrive twice and double-insert.
-- `users.user_pk is not null` matters: the join is a left join, so a learner who
-- no longer resolves (removed from dim_user, or a username change that moves the
-- join keys) would otherwise be "distinct from" the stored key and get re-inserted
-- with a null user_fk, replacing a good key with nothing. A row can be corrected
-- here, never nulled.
, stale_key_mitxonline as (
    select
        stored.platform
        , users.user_pk as user_fk
        , stored.openedx_user_id
        , stored.user_username
        , stored.courserun_readable_id
        , stored.event_type
        , stored.event_json
        , stored.problem_block_fk as problem_block_id
        , stored.answers
        , stored.attempt
        , stored.success
        , stored.grade
        , stored.max_grade
        , stored.event_timestamp
        , stored.event_timestamp_iso8601
        , stored.time_fk
        , stored.date_fk
    from {{ this }} as stored
    inner join watermarks on watermarks.platform = stored.platform
    left join users
        on
            stored.openedx_user_id = users.mitxonline_openedx_user_id
            and stored.user_username = users.user_mitxonline_username
    where
        stored.platform = 'mitxonline'
        and stored.event_timestamp <= watermarks.max_ts
        and users.user_pk is not null
        and users.user_pk is distinct from stored.user_fk
)

, stale_key_mitxpro as (
    select
        stored.platform
        , users.user_pk as user_fk
        , stored.openedx_user_id
        , stored.user_username
        , stored.courserun_readable_id
        , stored.event_type
        , stored.event_json
        , stored.problem_block_fk as problem_block_id
        , stored.answers
        , stored.attempt
        , stored.success
        , stored.grade
        , stored.max_grade
        , stored.event_timestamp
        , stored.event_timestamp_iso8601
        , stored.time_fk
        , stored.date_fk
    from {{ this }} as stored
    inner join watermarks on watermarks.platform = stored.platform
    left join users
        on
            stored.openedx_user_id = users.mitxpro_openedx_user_id
            and stored.user_username = users.user_mitxpro_username
    where
        stored.platform = 'mitxpro'
        and stored.event_timestamp <= watermarks.max_ts
        and users.user_pk is not null
        and users.user_pk is distinct from stored.user_fk
)

, stale_key_residential as (
    select
        stored.platform
        , users.user_pk as user_fk
        , stored.openedx_user_id
        , stored.user_username
        , stored.courserun_readable_id
        , stored.event_type
        , stored.event_json
        , stored.problem_block_fk as problem_block_id
        , stored.answers
        , stored.attempt
        , stored.success
        , stored.grade
        , stored.max_grade
        , stored.event_timestamp
        , stored.event_timestamp_iso8601
        , stored.time_fk
        , stored.date_fk
    from {{ this }} as stored
    inner join watermarks on watermarks.platform = stored.platform
    left join users
        on
            stored.openedx_user_id = users.residential_openedx_user_id
            and stored.user_username = users.user_residential_username
    where
        stored.platform = 'residential'
        and stored.event_timestamp <= watermarks.max_ts
        and users.user_pk is not null
        and users.user_pk is distinct from stored.user_fk
)

, stale_key_edxorg as (
    select
        stored.platform
        , users.user_pk as user_fk
        , stored.openedx_user_id
        , stored.user_username
        , stored.courserun_readable_id
        , stored.event_type
        , stored.event_json
        , stored.problem_block_fk as problem_block_id
        , stored.answers
        , stored.attempt
        , stored.success
        , stored.grade
        , stored.max_grade
        , stored.event_timestamp
        , stored.event_timestamp_iso8601
        , stored.time_fk
        , stored.date_fk
    from {{ this }} as stored
    inner join watermarks on watermarks.platform = stored.platform
    left join users
        on
            stored.openedx_user_id = users.edxorg_openedx_user_id
            and stored.user_username = users.user_edxorg_username
    where
        stored.platform = 'edxorg'
        and stored.event_timestamp <= watermarks.max_ts
        and users.user_pk is not null
        and users.user_pk is distinct from stored.user_fk
)

, stale_key_rows as (
    select * from stale_key_mitxonline
    union all
    select * from stale_key_mitxpro
    union all
    select * from stale_key_residential
    union all
    select * from stale_key_edxorg
)
{% endif %}

-- Studentmodule rows pre-aggregated to one row per (platform, user, course, problem, attempt)
-- before the union. tfact_studentmodule_problems is at per-submission grain (one row per
-- history record); aggregating here collapses multiple submissions for the same attempt to
-- the latest state, matching the per-attempt grain of this model. Without this aggregation,
-- 57M+ per-submission rows flow into the dedup window function unnecessarily.
--
-- row_number() over (...) replaces Trino-specific max_by() to keep the model portable
-- across DuckDB and other adapters.
, combined_studentmodule_ranked as (
    select
        sp.platform
        , sp.user_fk
        , sp.openedx_user_id
        , sp.user_username
        , sp.courserun_readable_id
        , 'problem_check' as event_type
        , sp.correct_map as event_json
        , sp.problem_block_id
        , sp.answers
        , sp.attempt
        , sp.success
        , sp.grade
        , sp.max_grade
        , sp.event_timestamp
        , sp.event_timestamp_iso8601
        , sp.time_fk
        , sp.date_fk
        , row_number() over (
            partition by sp.platform, sp.openedx_user_id, sp.courserun_readable_id, sp.problem_block_id, sp.attempt
            order by sp.event_timestamp desc
        ) as rn
    from {{ ref('tfact_studentmodule_problems') }} as sp
    {% if is_incremental() %}
    left join watermarks on watermarks.platform = sp.platform
    where (watermarks.max_ts is null or sp.event_timestamp > watermarks.max_ts)
    {% endif %}
)

, combined_studentmodule as (
    select
        platform
        , user_fk
        , openedx_user_id
        , user_username
        , courserun_readable_id
        , event_type
        , event_json
        , problem_block_id
        , answers
        , attempt
        , success
        , grade
        , max_grade
        , event_timestamp
        , event_timestamp_iso8601
        , time_fk
        , date_fk
    from combined_studentmodule_ranked
    where rn = 1
)

, combined as (
    select
        'mitxonline' as platform
        , users.user_pk as user_fk
        , mitxonline_problem_events.openedx_user_id
        , mitxonline_problem_events.user_username
        , mitxonline_problem_events.courserun_readable_id
        , mitxonline_problem_events.event_type
        , mitxonline_problem_events.event_json
        , mitxonline_problem_events.problem_block_id
        , mitxonline_problem_events.answers
        , mitxonline_problem_events.attempt
        , mitxonline_problem_events.success
        , mitxonline_problem_events.grade
        , mitxonline_problem_events.max_grade
        , mitxonline_problem_events.event_timestamp
        , mitxonline_problem_events.event_timestamp_iso8601
        , mitxonline_problem_events.time_fk
        , mitxonline_problem_events.date_fk
    from mitxonline_problem_events
    left join users
        on
            mitxonline_problem_events.openedx_user_id = users.mitxonline_openedx_user_id
            and mitxonline_problem_events.user_username = users.user_mitxonline_username

    union all

    select
        'mitxpro' as platform
        , users.user_pk as user_fk
        , xpro_problem_events.openedx_user_id
        , xpro_problem_events.user_username
        , xpro_problem_events.courserun_readable_id
        , xpro_problem_events.event_type
        , xpro_problem_events.event_json
        , xpro_problem_events.problem_block_id
        , xpro_problem_events.answers
        , xpro_problem_events.attempt
        , xpro_problem_events.success
        , xpro_problem_events.grade
        , xpro_problem_events.max_grade
        , xpro_problem_events.event_timestamp
        , xpro_problem_events.event_timestamp_iso8601
        , xpro_problem_events.time_fk
        , xpro_problem_events.date_fk
    from xpro_problem_events
    left join users
        on
            xpro_problem_events.openedx_user_id = users.mitxpro_openedx_user_id
            and xpro_problem_events.user_username = users.user_mitxpro_username

    union all

    select
        'residential' as platform
        , users.user_pk as user_fk
        , mitxresidential_problem_events.user_id
        , mitxresidential_problem_events.user_username
        , mitxresidential_problem_events.courserun_readable_id
        , mitxresidential_problem_events.event_type
        , mitxresidential_problem_events.event_json
        , mitxresidential_problem_events.problem_block_id
        , mitxresidential_problem_events.answers
        , mitxresidential_problem_events.attempt
        , mitxresidential_problem_events.success
        , mitxresidential_problem_events.grade
        , mitxresidential_problem_events.max_grade
        , mitxresidential_problem_events.event_timestamp
        , mitxresidential_problem_events.event_timestamp_iso8601
        , mitxresidential_problem_events.time_fk
        , mitxresidential_problem_events.date_fk
    from mitxresidential_problem_events
    left join users
        on
            mitxresidential_problem_events.user_id = users.residential_openedx_user_id
            and mitxresidential_problem_events.user_username = users.user_residential_username

    union all

    select
        'edxorg' as platform
        , users.user_pk as user_fk
        , edxorg_problem_events.user_id
        , edxorg_problem_events.user_username
        , edxorg_problem_events.courserun_readable_id
        , edxorg_problem_events.event_type
        , edxorg_problem_events.event_json
        , edxorg_problem_events.problem_block_id
        , edxorg_problem_events.answers
        , edxorg_problem_events.attempt
        , edxorg_problem_events.success
        , edxorg_problem_events.grade
        , edxorg_problem_events.max_grade
        , edxorg_problem_events.event_timestamp
        , edxorg_problem_events.event_timestamp_iso8601
        , edxorg_problem_events.time_fk
        , edxorg_problem_events.date_fk
    from edxorg_problem_events
    left join users
        on
            edxorg_problem_events.user_id = users.edxorg_openedx_user_id
            and edxorg_problem_events.user_username = users.user_edxorg_username

    union all

    select
        platform
        , user_fk
        , openedx_user_id
        , user_username
        , courserun_readable_id
        , event_type
        , event_json
        , problem_block_id
        , answers
        , attempt
        , success
        , grade
        , max_grade
        , event_timestamp
        , event_timestamp_iso8601
        , time_fk
        , date_fk
    from combined_studentmodule

    {% if is_incremental() %}
    union all

    select
        platform
        , user_fk
        , openedx_user_id
        , user_username
        , courserun_readable_id
        , event_type
        , event_json
        , problem_block_id
        , answers
        , attempt
        , success
        , grade
        , max_grade
        , event_timestamp
        , event_timestamp_iso8601
        , time_fk
        , date_fk
    from stale_key_rows
    {% endif %}
)

-- Deduplicate on (platform, user, course, problem, attempt):
--   - problem_check: keep the earliest event (rn=1) — tracking log events typically
--     have earlier timestamps than studentmodule events for the same submission
--   - showanswer and other types: keep all rows regardless of rank
, deduped_combined as (
    select *
    from (
        select
            *
            , row_number() over (
                partition by platform, openedx_user_id, courserun_readable_id, problem_block_id, attempt
                order by event_timestamp
            ) as rn
        from combined
    )
    where rn = 1 or event_type != 'problem_check'
)

select
    -- Surrogate key: unique per (platform, user, course, problem, attempt, event_type, timestamp).
    -- Includes event_timestamp so showanswer events (multiple per attempt) each get a distinct key,
    -- and problem_check events are idempotent across runs (same event → same key → no duplicate insert).
    {{ dbt_utils.generate_surrogate_key([
        'deduped_combined.platform',
        'deduped_combined.openedx_user_id',
        'deduped_combined.courserun_readable_id',
        'deduped_combined.problem_block_id',
        'deduped_combined.attempt',
        'deduped_combined.event_type',
        'deduped_combined.event_timestamp'
    ]) }} as event_id
    , platform.platform_pk as platform_fk
    , deduped_combined.user_fk
    , deduped_combined.platform
    , deduped_combined.openedx_user_id
    , deduped_combined.user_username
    , deduped_combined.courserun_readable_id
    , deduped_combined.event_type
    , deduped_combined.problem_block_id as problem_block_fk
    , deduped_combined.answers
    , deduped_combined.attempt
    , deduped_combined.success
    , deduped_combined.grade
    , deduped_combined.max_grade
    , deduped_combined.event_timestamp
    , deduped_combined.event_timestamp_iso8601
    , deduped_combined.time_fk
    , deduped_combined.date_fk
    , deduped_combined.event_json
from deduped_combined
left join platform on deduped_combined.platform = platform.platform_readable_id
