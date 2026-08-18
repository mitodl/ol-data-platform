-- Transactional fact at turn grain: one row per atomic free-text feedback utterance.
-- INSERT-ONLY -- nothing writes to a row after it lands. Every model-generated
-- attribute (summary, embedding, category, sentiment, cluster) lives on
-- afact_feedback_conversation instead, so the inferred layer can be rebuilt without
-- touching the record of what people actually wrote.
with unioned as (
    select * from {{ ref('int__feedback__unioned') }}
)

-- Joined on the email COLUMN, not on dim_user's pk formula -- user_pk is keyed on
-- (user_identity_source, user_identity_id), so a Zendesk requester who also has an
-- openedx account resolves to their real identity rather than an email-keyed duplicate.
-- dim_user is unique on email today but carries no unique test to enforce it; if that
-- ever changes this join fans out and feedback_pk's unique test fails, which is the
-- loud failure we want rather than a silent arbitrary pick.
, users as (
    select
        user_pk
        , lower(email) as email
    from {{ ref('dim_user') }}
    where email is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['unioned.source_slug', 'unioned.source_record_ref']) }}
        as feedback_pk
    , {{ dbt_utils.generate_surrogate_key(['unioned.source_slug']) }} as feedback_source_fk
    , {{ dbt_utils.generate_surrogate_key(['unioned.channel_slug']) }} as feedback_channel_fk
    , users.user_pk as user_fk
    -- Zendesk resolves none of these; they are populated by the course-scoped sources
    -- that arrive in Phase 2 (design 2b explains organization_fk specifically)
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
    -- kept, not consumed-and-dropped: this fact is insert-only, so an author unmatched
    -- at load time stays unmatched forever unless the raw ref survives to re-resolve
    -- user_fk later without a rebuild
    , unioned.subject_user_ref
    -- Populated by the feedback_redacted Dagster asset (spec 3), which masks PII before
    -- text reaches this fact. Until it lands the fact carries the columns but no text --
    -- deliberately, rather than carrying unredacted text into a broadly-granted schema.
    , cast(null as varchar) as feedback_title
    , cast(null as varchar) as feedback_text
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
