{{ config(
    materialized='incremental',
    unique_key='feedback_pk',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

with unioned as (
    select
        *
        , {{ json_query_string('source_metadata', "'$.courserun_platform'") }}
            as courserun_platform
    from {{ ref('int__feedback__unioned') }}
)

-- dim_user is unique on email but has no test enforcing it; if that changes, this join
-- fans out and feedback_pk's unique test fails, which is the failure we want.
, dim_course_run as (
    select courserun_pk, courserun_readable_id, platform
    from {{ ref('dim_course_run') }}
    where is_current = true
)

, dim_course_content as (
    select content_block_pk, block_id, platform
    from {{ ref('dim_course_content') }}
    where is_latest = true
)

, users as (
    select
        user_pk
        , lower(email) as email
    from {{ ref('dim_user') }}
    where email is not null
)

{% set redacted = dev_schema_source('feedback_intermediate', 'feedback_redacted') %}

, redacted as (
{% if not redacted.is_unit_test and execute and not redacted.resolved_relation %}
    select
        cast(null as varchar) as source_slug
        , cast(null as varchar) as source_record_ref
        , cast(null as varchar) as title_redacted
        , cast(null as varchar) as text_redacted
    where false
{% else %}
    select * from {{ redacted.relation_ref }}
{% endif %}
)

select
    {{ dbt_utils.generate_surrogate_key(['unioned.source_slug', 'unioned.source_record_ref']) }}
        as feedback_pk
    , {{ dbt_utils.generate_surrogate_key(['unioned.source_slug']) }} as feedback_source_fk
    , {{ dbt_utils.generate_surrogate_key(['unioned.channel_slug']) }} as feedback_channel_fk
    , users.user_pk as user_fk
    , dim_course_run.courserun_pk as courserun_fk
    , dim_course_content.content_block_pk as content_block_fk
    -- the case keeps a null platform null; generate_surrogate_key would hash it
    , case
        when unioned.platform is not null
            then {{ dbt_utils.generate_surrogate_key(['unioned.platform']) }}
    end as platform_fk
    , cast(null as varchar) as organization_fk
    , {{ iso8601_to_date_key('unioned.occurred_at') }} as occurred_date_fk
    , {{ iso8601_to_time_key('unioned.occurred_at') }} as occurred_time_fk
    , {{ iso8601_to_date_key('unioned.created_at') }} as created_date_fk
    , {{ iso8601_to_time_key('unioned.created_at') }} as created_time_fk
    , {{ iso8601_to_date_key('unioned.updated_at') }} as updated_date_fk
    , {{ iso8601_to_time_key('unioned.updated_at') }} as updated_time_fk
    , unioned.conversation_ref as conversation_id
    , unioned.turn_index
    , unioned.is_conversation_opening
    , unioned.source_record_ref as source_record_id
    , unioned.source_url
    , unioned.subject_type
    , unioned.subject_ref
    , unioned.subject_url
    -- kept so user_fk can be re-resolved later without a rebuild
    , unioned.subject_user_ref
    , redacted.title_redacted as feedback_title
    , redacted.text_redacted as feedback_text
    , unioned.feedback_text_chars
    , unioned.explicit_rating
    , unioned.source_metadata
    , unioned.occurred_at as feedback_occurred_at
    , unioned.created_at as feedback_created_at
    , unioned.updated_at as feedback_updated_at
    , {{ cast_timestamp_to_iso8601('current_timestamp') }} as feedback_ingested_at
from unioned
left join users
    on lower(unioned.subject_user_ref) = users.email
left join dim_course_run
    on unioned.courserun_readable_id = dim_course_run.courserun_readable_id
    and unioned.courserun_platform = dim_course_run.platform
left join dim_course_content
    on unioned.block_id = dim_course_content.block_id
    and unioned.courserun_platform = dim_course_content.platform
left join redacted
    on unioned.source_slug = redacted.source_slug
    and unioned.source_record_ref = redacted.source_record_ref
{% if is_incremental() %}
    -- Watermark on the CONVERSATION's updated_at, not the turn's: a ticket that gains a
    -- rating, a status change or a late-syncing comment re-enters with all of its turns,
    -- so delete+insert replaces them together. Filtering to unseen turns instead would
    -- freeze the ticket-level columns and leave turn_index inconsistent.
    --
    -- Per source, not a single global max: a global watermark would compare every
    -- source's rows against whichever source is currently newest, so a newly-added
    -- source's entire (older) history would never pass the filter. coalesce's fallback
    -- covers a source with no rows in the table yet, so its first run backfills
    -- everything instead of needing a manual --full-refresh.
    where unioned.updated_at > coalesce(
        (
            select max(stale.feedback_updated_at)
            from {{ this }} as stale
            where stale.feedback_source_fk
                = {{ dbt_utils.generate_surrogate_key(['unioned.source_slug']) }}
        ),
        '0001-01-01T00:00:00'
    )
    -- Backfill: a row inserted before feedback_redacted existed carries the old
    -- feedback_text = null stub forever under the watermark above alone, because
    -- redaction landing does not bump the source ticket's updated_at. Reselect any
    -- row still null in the fact where redaction has since produced real text.
    or exists (
        select 1
        from {{ this }} as stale
        inner join redacted
            on stale.source_record_id = redacted.source_record_ref
            and stale.feedback_source_fk = {{ dbt_utils.generate_surrogate_key(['redacted.source_slug']) }}
        where stale.source_record_id = unioned.source_record_ref
            and stale.feedback_source_fk = {{ dbt_utils.generate_surrogate_key(['unioned.source_slug']) }}
            and stale.feedback_text is null
            and redacted.text_redacted is not null
    )
    -- dim_user re-key: a stored user_fk that no longer matches the current dim_user
    -- resolution means the turn's subject_user_ref was re-keyed after ingestion, and
    -- the source row's updated_at won't have moved to trigger the watermark above.
    or exists (
        select 1
        from {{ this }} as stale
        where stale.feedback_pk = {{ dbt_utils.generate_surrogate_key(['unioned.source_slug', 'unioned.source_record_ref']) }}
            and stale.user_fk is distinct from users.user_pk
    )
{% endif %}
