{{ config(
    materialized='incremental',
    unique_key='problem_attempt_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    partition_by=(['platform'] if target.type == 'starrocks' else none),
    properties=(
        none if target.type == 'starrocks'
        else {"partitioning": "ARRAY['platform']"}
    ),
) }}

{% set problem_events =
    (
    'problem_check'
    , 'showanswer'
    )
%}

{#-
    One entry per tracking-log source. `load_id` marks the dlt-loaded sources, whose
    staging rows carry the load that wrote them (useractivity_load_id). edX.org's raw
    table is frozen (ingestion/inventory/retired.yml), so no row can arrive late there
    and it keeps the event-time watermark.
-#}
{%- set tracking_log_sources = [
    {
        'platform': 'mitxonline',
        'model': 'stg__mitxonline__openedx__tracking_logs__user_activity',
        'user_id': 'openedx_user_id',
        'courserun': 'tracking_logs.courserun_readable_id',
        'load_id': true,
    },
    {
        'platform': 'mitxpro',
        'model': 'stg__mitxpro__openedx__tracking_logs__user_activity',
        'user_id': 'openedx_user_id',
        'courserun': 'tracking_logs.courserun_readable_id',
        'load_id': true,
    },
    {
        'platform': 'residential',
        'model': 'stg__mitxresidential__openedx__tracking_logs__user_activity',
        'user_id': 'user_id',
        'courserun': 'tracking_logs.courserun_readable_id',
        'load_id': true,
    },
    {
        'platform': 'edxorg',
        'model': 'stg__edxorg__s3__tracking_logs__user_activity',
        'user_id': 'user_id',
        'courserun': format_course_id('tracking_logs.courserun_readable_id'),
        'load_id': false,
    },
] -%}

{#-
    Incremental bounds, read once per run.

    A dlt source re-reads every staging row loaded at or after the newest stored load id
    less a 3-hour lookback (dlt_load_id_floor). Its event time can be anything, so a
    tracking-log file that lands days late still reaches this table on the next run.
    An event-time watermark skipped those rows for good.

    edX.org, and a dlt source with no stored load id yet, use the event-time watermark
    (max event_timestamp) instead. The literal date floor on the ISO 8601 text, a day
    before the watermark's date, is a scan predicate: StarRocks applies it to the Iceberg
    scan's min/max pruning, so old files are skipped. Without it, every historical
    problem event was read with both JSON columns, and a StarRocks CN ran out of process
    memory (13.9 GB) while the query's tracked memory stayed near 2 GB, so
    query_mem_limit could not stop it. The load id literal prunes the same way. A day
    early because staging text written by Trino keeps the event's UTC offset, so its
    date can be a day behind the UTC date.

    The table must already have problem_attempt_key, the unique_key. Rows stored before
    it was added have it null, so delete+insert would never replace them, and every
    re-read row would be stored twice. Adding it takes a --full-refresh.
-#}
{%- set source_date_floor = {} -%}
{#- A unit test's {{ this }} is a fixture CTE, not a table run_query can read, so a
    unit test sets the load floor per platform with this var instead. -#}
{%- set source_load_floor = {} -%}
{%- do source_load_floor.update(var('tfact_problem_events_unit_test_load_floor', {})) -%}
{%- if execute and is_incremental() and model.resource_type != 'unit_test' -%}
    {%- set stored_columns = adapter.get_columns_in_relation(this) | map(attribute='name') | map('lower') | list -%}
    {%- if 'problem_attempt_key' not in stored_columns -%}
        {{ exceptions.raise_compiler_error(
            this ~ " has no problem_attempt_key column. Build it with --full-refresh."
        ) }}
    {%- endif -%}
    {%- for row in run_query(
        'select platform, max(event_timestamp), max(source_load_id) from ' ~ this ~ ' group by platform'
    ) -%}
        {%- if row[1] is not none -%}
            {%- do source_date_floor.update({
                row[0]: (
                    modules.datetime.date.fromisoformat((row[1] | string)[:10])
                    - modules.datetime.timedelta(days=1)
                ).isoformat()
            }) -%}
        {%- endif -%}
        {%- if row[2] is not none -%}
            {%- do source_load_floor.update({row[0]: dlt_load_id_floor(row[2])}) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif %}

with users as (
    select * from {{ ref('dim_user') }}
)

, platform as (
    select * from {{ ref('dim_platform') }}
)

{% if is_incremental() %}
, watermarks as (
    select platform, max(event_timestamp) as max_ts
    from {{ this }}
    group by platform
)
{% endif %}

-- data from tracking logs
{%- for source in tracking_log_sources %}

, {{ source.platform }}_problem_events as (
    select
        '{{ source.platform }}' as platform
        , users.user_pk as user_fk
        , tracking_logs.{{ source.user_id }} as openedx_user_id
        , tracking_logs.user_username
        , {{ source.courserun }} as courserun_readable_id
        , tracking_logs.useractivity_event_type as event_type
        , tracking_logs.useractivity_event_object as event_json
        , {{ json_query_string('tracking_logs.useractivity_event_object', "'$.problem_id'") }} as problem_block_id
        , {{ json_query_object('tracking_logs.useractivity_event_object', 'answers') }} as answers
        , {{ json_query_string('tracking_logs.useractivity_event_object', "'$.attempts'") }} as attempt
        , {{ json_query_string('tracking_logs.useractivity_event_object', "'$.success'") }} as success
        , {{ json_query_string('tracking_logs.useractivity_event_object', "'$.grade'") }} as grade
        , {{ json_query_string('tracking_logs.useractivity_event_object', "'$.max_grade'") }} as max_grade
        , {{ from_iso8601_timestamp_nanos('tracking_logs.useractivity_timestamp') }} as event_timestamp
        , tracking_logs.useractivity_timestamp as event_timestamp_iso8601
        , {{ iso8601_to_time_key('tracking_logs.useractivity_timestamp') }} as time_fk
        , {{ iso8601_to_date_key('tracking_logs.useractivity_timestamp') }} as date_fk
        {%- if source.load_id %}
        , tracking_logs.useractivity_load_id as source_load_id
        {%- else %}
        , cast(null as varchar) as source_load_id
        {%- endif %}
    from {{ ref(source.model) }} as tracking_logs
    {%- if is_incremental() and not (source.load_id and source.platform in source_load_floor) %}
    left join watermarks on watermarks.platform = '{{ source.platform }}'
    {%- endif %}
    left join users
        on
            tracking_logs.{{ source.user_id }} = users.{{ source.platform }}_openedx_user_id
            and tracking_logs.user_username = users.user_{{ source.platform }}_username
    where
        tracking_logs.courserun_readable_id is not null
        and tracking_logs.useractivity_event_type in {{ problem_events }}
        and tracking_logs.useractivity_event_source = 'server'
        {%- if is_incremental() %}
        {%- if source.load_id and source.platform in source_load_floor %}
        and tracking_logs.useractivity_load_id >= '{{ source_load_floor[source.platform] }}'
        {%- else %}
        {%- if source.platform in source_date_floor %}
        and tracking_logs.useractivity_timestamp >= '{{ source_date_floor[source.platform] }}'
        {%- endif %}
        and (
            watermarks.max_ts is null
            or {{ from_iso8601_timestamp_nanos('tracking_logs.useractivity_timestamp') }} > watermarks.max_ts
        )
        {%- endif %}
        {%- endif %}
)
{%- endfor %}

{% if is_incremental() %}
-- Rows already in the target whose stored user_fk no longer matches what dim_user
-- resolves today. The sources above only re-select events loaded since the last run,
-- so without this a dim_user re-key strands historical activity under the obsolete
-- key -- the same hazard tfact_grade and tfact_certificate guard against with their
-- stale_user_fk_lookup. delete+insert on problem_attempt_key replaces the row in
-- place, which only works because these rows keep their stored keys (see candidates
-- below).
-- Two CTEs per platform because the dim_user join keys differ per platform; each
-- prunes to one partition (the model is partitioned by platform) and joins by
-- equality rather than an OR across every platform's key pair.
-- A stale row is one more candidate in the dedup below. When a new row for the same
-- attempt is earlier, the new row wins with the user_fk its own source resolved: a
-- tracking-log row from dim_user, a studentmodule row from tfact_studentmodule_problems.
-- stale_tuples_* compares dim_user with the distinct stored (user id, username,
-- user_fk) tuples, not with every stored event: about 700k tuples against 160M rows
-- (measured 2026-11-20). Joining every event to dim_user ran StarRocks CNs out of
-- memory: each platform's dim_user hash table was about 1 GB per CN at dop 1, and
-- the edX.org join's memory grew with dop (4.23 GB at dop 4). stale_key_* then
-- joins the stored events to the few stale tuples. The result is the same as the
-- per-event join.
-- The inner join (and `users.user_pk is not null`) keeps a learner who no longer
-- resolves (removed from dim_user, or a username change that moves the join keys)
-- out of the stale set; re-inserting them with a null user_fk would replace a good
-- key with nothing. A row can be corrected here, never nulled. The platform id
-- filter changes no result (a matched row has a non-null id), but keeps the users
-- with no account on that platform out of the join.
{%- set stale_key_platforms = ['mitxonline', 'mitxpro', 'residential', 'edxorg'] %}
{%- for stale_platform in stale_key_platforms %}

, stale_tuples_{{ stale_platform }} as (
    select
        stored_keys.openedx_user_id
        , stored_keys.user_username
        , stored_keys.user_fk as stored_user_fk
        , users.user_pk
    from (
        select distinct
            stored.openedx_user_id
            , stored.user_username
            , stored.user_fk
        from {{ this }} as stored
        where stored.platform = '{{ stale_platform }}'
    ) as stored_keys
    inner join users
        on
            stored_keys.openedx_user_id = users.{{ stale_platform }}_openedx_user_id
            and stored_keys.user_username = users.user_{{ stale_platform }}_username
    where
        users.user_pk is not null
        and users.{{ stale_platform }}_openedx_user_id is not null
        and {{ is_distinct_from('users.user_pk', 'stored_keys.user_fk') }}
)

, stale_key_{{ stale_platform }} as (
    select
        stored.event_id
        , stored.problem_attempt_key
        , stored.platform
        , stale.user_pk as user_fk
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
        , stored.source_load_id
    from {{ this }} as stored
    inner join stale_tuples_{{ stale_platform }} as stale
        on
            stored.openedx_user_id = stale.openedx_user_id
            and stored.user_username = stale.user_username
            and not ({{ is_distinct_from('stored.user_fk', 'stale.stored_user_fk') }})
    where stored.platform = '{{ stale_platform }}'
)
{%- endfor %}

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
        , cast(null as varchar) as source_load_id
    from combined_studentmodule_ranked
    where rn = 1
)

, combined as (
    {%- for source in tracking_log_sources %}
    select * from {{ source.platform }}_problem_events
    union all
    {%- endfor %}
    select * from combined_studentmodule
)

-- event_id: unique per (platform, user, course, problem, attempt, event_type, timestamp).
-- Includes event_timestamp so showanswer events (multiple per attempt) each get a
-- distinct key.
-- problem_attempt_key, the unique_key: one problem_check row per (platform, user,
-- course, problem, attempt), and each showanswer event on its own (its event_id).
-- Keying problem_check by the attempt lets delete+insert replace the stored row of an
-- attempt when a run finds an earlier event for it, e.g. a tracking-log row that landed
-- after the studentmodule row for the same attempt was stored.
, combined_with_event_id as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'combined.platform',
            'combined.openedx_user_id',
            'combined.courserun_readable_id',
            'combined.problem_block_id',
            'combined.attempt',
            'combined.event_type',
            surrogate_key_timestamptz('combined.event_timestamp', 'combined.event_timestamp_iso8601')
        ]) }} as event_id
        , combined.*
    from combined
)

, keyed as (
    select
        case
            when event_type = 'problem_check'
                then {{ dbt_utils.generate_surrogate_key([
                    'platform',
                    'openedx_user_id',
                    'courserun_readable_id',
                    'problem_block_id',
                    'attempt',
                    'event_type',
                ]) }}
            else event_id
        end as problem_attempt_key
        , *
        , 0 as is_stored
    from combined_with_event_id
)

{% if is_incremental() %}
-- The stored problem_check row of each attempt this run touches, so the dedup below
-- compares the new rows with it. Without it, a late tracking-log row and the stored
-- studentmodule row for the same attempt would both stay.
, stored_problem_checks as (
    select
        stored.event_id
        , stored.problem_attempt_key
        , stored.platform
        , stored.user_fk
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
        , stored.source_load_id
        , 1 as is_stored
        , 0 as is_corrected
    from {{ this }} as stored
    inner join (
        select distinct problem_attempt_key
        from keyed
        where event_type = 'problem_check'
    ) as touched
        on stored.problem_attempt_key = touched.problem_attempt_key
    where stored.event_type = 'problem_check'
)
{% endif %}

-- New rows, the stored rows of the attempts they touch, and the stored rows whose
-- user_fk dim_user has re-keyed. Stored rows keep their stored keys, not a rehash:
-- event_timestamp reads back in UTC, while the source path hashed it with its
-- original offset, so a rehash would change the event_id.
, candidates as (
    select
        event_id
        , problem_attempt_key
        , platform
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
        , source_load_id
        , is_stored
        , 0 as is_corrected
    from keyed
    {% if is_incremental() %}
    union all
    select * from stored_problem_checks
    union all
    select
        event_id
        , problem_attempt_key
        , platform
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
        , source_load_id
        , 1 as is_stored
        , 1 as is_corrected
    from stale_key_rows
    {% endif %}
)

-- One row per problem_attempt_key: for problem_check the earliest event of the attempt
-- (tracking log events typically have earlier timestamps than studentmodule events for
-- the same submission). On a tie the stored row wins, so its event_id stays put, and
-- of two stored copies the one with the corrected user_fk. A showanswer's key is its
-- own event_id, so for it this only drops repeats of the same event.
, deduped as (
    select *
    from (
        select
            *
            , row_number() over (
                partition by problem_attempt_key
                order by event_timestamp asc nulls last, is_stored desc, is_corrected desc
            ) as rn
        from candidates
    ) as subquery
    where rn = 1
)

, batch as (
    select
        event_id
        , problem_attempt_key
        , platform
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
        , source_load_id
    from deduped
)

-- StarRocks: this query's memory grows with the pipeline dop. At the default dop (8
-- here) it exceeded a 6 GiB query limit while reading and decompressing Iceberg
-- pages; at dop 2 it peaked at 3.7 GB per CN and took 42s (measured 2026-11-20).
-- A full refresh runs this same query, so it is held to dop 2 as well.
select
    {{ session_variables_hint({'pipeline_dop': 2}) }}
    batch.event_id
    , platform.platform_pk as platform_fk
    , batch.user_fk
    , batch.platform
    , batch.openedx_user_id
    , batch.user_username
    , batch.courserun_readable_id
    , batch.event_type
    , batch.problem_block_id as problem_block_fk
    , batch.answers
    , batch.attempt
    , batch.success
    , batch.grade
    , batch.max_grade
    , batch.event_timestamp
    , batch.event_timestamp_iso8601
    , batch.time_fk
    , batch.date_fk
    , batch.event_json
    , batch.problem_attempt_key
    , batch.source_load_id
from batch
left join platform on batch.platform = platform.platform_readable_id
