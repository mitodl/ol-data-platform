with conversation_fact as (
    select * from {{ ref('afact_feedback_conversation') }}
    where is_in_cluster_scope
)

, feedback_category as (
    select * from {{ ref('dim_feedback_category') }}
)

, feedback_cluster as (
    select * from {{ ref('dim_feedback_cluster') }}
)

, feedback_source as (
    select * from {{ ref('dim_feedback_source') }}
)

, opening_turn as (
    select * from {{ ref('tfact_feedback') }}
    where is_conversation_opening
)

, date_dim as (
    select * from {{ ref('dim_date') }}
)

, course_run as (
    select * from {{ ref('dim_course_run') }}
    where is_current
)

select
    conversation_fact.cluster_key
    , feedback_category.category_label
    , feedback_cluster.category_description
    , feedback_source.source_name as source
    , {{ json_extract_scalar('opening_turn.source_metadata', "'$.chatsession_agent'") }}
        as chat_agent
    , conversation_fact.conversation_id
    , date_dim.date as conversation_date
    , {{ format_datetime('date_dim.date', 'yyyy-MM') }} as conversation_month
    , {{ format_datetime('date_dim.date', 'yyyy') }} as conversation_year
    , opening_turn.source_url
    , conversation_fact.conversation_summary
    -- Keeps quoted email history: it is often the only copy of the email a learner
    -- replied to, and the summarizer reads the same full text. HTML, because the
    -- Feedback Cluster Conversations chart renders it to show line breaks.
    , replace(
        {{ text_to_html(html_unescape('conversation_fact.conversation_text')) }}
        , '<br>---<br>', '<br><br>'
    ) as conversation_text
    , course_run.courserun_readable_id
    , course_run.courserun_title as course_title
    , count(*) over (partition by conversation_fact.cluster_key) as cluster_size
from conversation_fact
inner join feedback_category
    on conversation_fact.category_fk = feedback_category.feedback_category_pk
-- The category_fk match keeps only conversations whose category came from their
-- cluster, not from the tag-seed fallback.
inner join feedback_cluster
    on
        conversation_fact.cluster_key = feedback_cluster.cluster_key
        and conversation_fact.category_fk = feedback_cluster.category_fk
inner join feedback_source
    on conversation_fact.feedback_source_fk = feedback_source.feedback_source_pk
inner join opening_turn
    on
        conversation_fact.conversation_id = opening_turn.conversation_id
        and conversation_fact.feedback_source_fk = opening_turn.feedback_source_fk
left join date_dim
    on conversation_fact.opened_date_fk = date_dim.date_key
left join course_run
    on conversation_fact.courserun_fk = course_run.courserun_pk
