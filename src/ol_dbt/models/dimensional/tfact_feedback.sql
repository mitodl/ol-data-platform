-- INSERT-ONLY: every model-generated attribute lives on afact_feedback_conversation,
-- so the inferred layer can be rebuilt without touching this fact.
with unioned as (
    select * from {{ ref('int__feedback__unioned') }}
)

-- Joined on the email COLUMN, not on dim_user's pk formula, so a requester who also has
-- an openedx account resolves to their real identity rather than an email-keyed
-- duplicate. dim_user is unique on email but has no test enforcing it; if that changes
-- this join fans out and feedback_pk's unique test fails, which is the failure we want.
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
    -- kept, not consumed-and-dropped: the fact is insert-only, so user_fk can only be
    -- re-resolved later if the raw ref survives
    , unioned.subject_user_ref
    -- Null rather than raw text: this schema is broadly granted and the text is unmasked
    -- upstream. These two nulls STUB the join the spec asks for, because the Presidio
    -- Dagster asset that produces the redacted text does not exist yet.
    --
    -- To remove the stub when feedback_redacted lands: declare it as a dbt source (as
    -- models/reporting/_reporting__sources.yml does for the Dagster-materialized
    -- student_risk_probability table), select redacted.title_redacted and
    -- redacted.text_redacted here, and add
    --     left join the feedback_redacted source as redacted
    --         on unioned.source_slug = redacted.source_slug
    --         and unioned.source_record_ref = redacted.source_record_ref
    -- Nothing downstream changes: int__feedback__conversation already reads this column.
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
