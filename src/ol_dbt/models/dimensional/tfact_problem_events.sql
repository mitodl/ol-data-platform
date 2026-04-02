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

, mitxonline_watermark as (select max_ts from watermarks where platform = 'mitxonline')
, mitxpro_watermark as (select max_ts from watermarks where platform = 'mitxpro')
, residential_watermark as (select max_ts from watermarks where platform = 'residential')
, edxorg_watermark as (select max_ts from watermarks where platform = 'edxorg')

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
    from {{ ref('stg__mitxonline__openedx__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ problem_events }}
        and useractivity_event_source = 'server'
        and {{ from_iso8601_timestamp_nanos('useractivity_timestamp') }}
            > (select max_ts from mitxonline_watermark)
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
    from {{ ref('stg__mitxpro__openedx__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ problem_events }}
        and useractivity_event_source = 'server'
        and {{ from_iso8601_timestamp_nanos('useractivity_timestamp') }}
            > (select max_ts from mitxpro_watermark)
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
    from {{ ref('stg__mitxresidential__openedx__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ problem_events }}
        and useractivity_event_source = 'server'
        and {{ from_iso8601_timestamp_nanos('useractivity_timestamp') }}
            > (select max_ts from residential_watermark)
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
    from {{ ref('stg__edxorg__s3__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ problem_events }}
        and useractivity_event_source = 'server'
        and {{ from_iso8601_timestamp_nanos('useractivity_timestamp') }}
            > (select max_ts from edxorg_watermark)
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

-- Studentmodule rows pre-aggregated to one row per (platform, user, course, problem, attempt)
-- before the union. tfact_studentmodule_problems is at per-submission grain (one row per
-- history record); aggregating here collapses multiple submissions for the same attempt to
-- the latest state, matching the per-attempt grain of this model. Without this aggregation,
-- 57M+ per-submission rows flow into the dedup window function unnecessarily.
, combined_studentmodule as (
    select
        platform
        , max_by(user_fk, event_timestamp) as user_fk
        , openedx_user_id
        , max_by(user_username, event_timestamp) as user_username
        , courserun_readable_id
        , 'problem_check' as event_type
        , max_by(correct_map, event_timestamp) as event_json
        , problem_block_id
        , max_by(answers, event_timestamp) as answers
        , attempt
        , max_by(success, event_timestamp) as success
        , max_by(grade, event_timestamp) as grade
        , max_by(max_grade, event_timestamp) as max_grade
        , max(event_timestamp) as event_timestamp
    from {{ ref('tfact_studentmodule_problems') }}
    {% if is_incremental() %}
    where (
        (platform = 'mitxonline'
            and event_timestamp > (select max_ts from mitxonline_watermark))
        or (platform = 'mitxpro'
            and event_timestamp > (select max_ts from mitxpro_watermark))
        or (platform = 'residential'
            and event_timestamp > (select max_ts from residential_watermark))
    )
    {% endif %}
    group by platform, openedx_user_id, courserun_readable_id, problem_block_id, attempt
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
    from combined_studentmodule
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
    , deduped_combined.event_json
from deduped_combined
left join platform on deduped_combined.platform = platform.platform_readable_id
