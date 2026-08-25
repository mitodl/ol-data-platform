{{ config(
    materialized='incremental',
    unique_key='feedback_pk',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

with unioned as (
    select * from {{ ref('int__feedback__unioned') }}
)

-- dim_user is unique on email but has no test enforcing it; if that changes, this join
-- fans out and feedback_pk's unique test fails, which is the failure we want.
, users as (
    select
        user_pk
        , lower(email) as email
    from {{ ref('dim_user') }}
    where email is not null
)

, redacted as (
    select * from {{ source('feedback_intermediate', 'feedback_redacted') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['unioned.source_slug', 'unioned.source_record_ref']) }}
        as feedback_pk
    , {{ dbt_utils.generate_surrogate_key(['unioned.source_slug']) }} as feedback_source_fk
    , {{ dbt_utils.generate_surrogate_key(['unioned.channel_slug']) }} as feedback_channel_fk
    , users.user_pk as user_fk
    -- Zendesk resolves none of these; the course-scoped sources populate them
    , cast(null as varchar) as courserun_fk
    , cast(null as varchar) as content_block_fk
    , cast(null as varchar) as platform_fk
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
left join redacted
    on unioned.source_slug = redacted.source_slug
    and unioned.source_record_ref = redacted.source_record_ref
{% if is_incremental() %}
    -- Watermark on the CONVERSATION's updated_at, not the turn's: a ticket that gains a
    -- rating, a status change or a late-syncing comment re-enters with all of its turns,
    -- so delete+insert replaces them together. Filtering to unseen turns instead would
    -- freeze the ticket-level columns and leave turn_index inconsistent.
    where unioned.updated_at > (select max(feedback_updated_at) from {{ this }})
    -- Backfill: a row inserted before feedback_redacted existed carries the old
    -- feedback_text = null stub forever under the watermark above alone, because
    -- redaction landing does not bump the source ticket's updated_at. Reselect any
    -- row still null in the fact where redaction has since produced real text.
    or exists (
        select 1
        from {{ this }} as stale
        inner join redacted
            on stale.source_record_id = redacted.source_record_ref
        where stale.source_record_id = unioned.source_record_ref
            and stale.feedback_text is null
            and redacted.text_redacted is not null
    )
{% endif %}
