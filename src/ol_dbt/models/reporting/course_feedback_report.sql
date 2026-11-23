-- What learners said about a course run, for its course team: AskTIM chats, content
-- feedback and forum threads. No user key, so an author can't be looked up through
-- dim_user. Its Superset dataset belongs under the instructor_course_scope RLS policy
-- (src/ol_superset/policies/ol_rls_policies.json), keyed on courserun_readable_id.
-- Unlike feedback_conversation_report it is not limited to the current cluster run.
with conversation_fact as (
    select * from {{ ref('afact_feedback_conversation') }}
)

-- An allow-list, so a new source reaches course teams only when added here. Zendesk
-- tickets are support conversations about a learner (refunds, ID checks), not course
-- feedback, even if they later gain a course run.
, feedback_source as (
    select * from {{ ref('dim_feedback_source') }}
    where source_slug in ('learn_ai_tutor', 'content_feedback', 'discussion_forum')
)

, opening_turn as (
    select * from {{ ref('tfact_feedback') }}
    where is_conversation_opening
)

, feedback_category as (
    select * from {{ ref('dim_feedback_category') }}
)

, sentiment as (
    select * from {{ ref('dim_sentiment') }}
)

, date_dim as (
    select * from {{ ref('dim_date') }}
)

, course_run as (
    select * from {{ ref('dim_course_run') }}
    where is_current
)

select
    course_run.courserun_readable_id
    , course_run.courserun_title as course_title
    , course_run.platform
    , feedback_source.source_name as source
    , {{ json_extract_scalar('opening_turn.source_metadata', "'$.chatsession_agent'") }}
        as chat_agent
    , conversation_fact.conversation_id
    , date_dim.date as conversation_date
    , {{ format_datetime('date_dim.date', 'yyyy-MM') }} as conversation_month
    , conversation_fact.turn_count
    , conversation_fact.explicit_rating
    , sentiment.sentiment_slug as sentiment
    , feedback_category.category_label
    -- The learner's browser URL is not redacted, so drop any query string or fragment
    -- a tracker may have appended.
    , split_part(split_part(opening_turn.subject_url, '?', 1), '#', 1) as content_url
    , conversation_fact.conversation_summary
    , conversation_fact.conversation_text
from conversation_fact
inner join feedback_source
    on conversation_fact.feedback_source_fk = feedback_source.feedback_source_pk
-- Inner: a conversation with no resolved course run has no row an RLS clause on
-- courserun_readable_id could ever show.
inner join course_run
    on conversation_fact.courserun_fk = course_run.courserun_pk
inner join opening_turn
    on
        conversation_fact.conversation_id = opening_turn.conversation_id
        and conversation_fact.feedback_source_fk = opening_turn.feedback_source_fk
left join date_dim
    on conversation_fact.opened_date_fk = date_dim.date_key
left join feedback_category
    on conversation_fact.category_fk = feedback_category.feedback_category_pk
left join sentiment
    on conversation_fact.sentiment_fk = sentiment.sentiment_pk
