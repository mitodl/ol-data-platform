-- Grain: one public, requester-authored comment = one turn.
with ticket_comment as (
    select * from {{ ref('int__zendesk__ticket_comment') }}
)

, ticket as (
    select * from {{ ref('int__zendesk__ticket') }}
)

, requester_turns as (
    select
        ticket_comment.comment_id
        , ticket_comment.ticket_id
        , ticket_comment.comment_plain_body
        , ticket_comment.comment_author_email
        , ticket_comment.comment_source_channel
        , ticket_comment.comment_created_at
        -- comment_id breaks ties: two comments sharing a created_at would otherwise get
        -- a nondeterministic turn_index and fail the compound-uniqueness test
        , row_number() over (
            partition by ticket_comment.ticket_id
            order by ticket_comment.comment_created_at, ticket_comment.comment_id
        ) as turn_index
    from ticket_comment
    inner join ticket on ticket_comment.ticket_id = ticket.ticket_id
    where
        ticket_comment.comment_is_public = true
        and ticket_comment.comment_author_user_id = ticket.ticket_requester_user_id
        -- Zendesk's own demo content, not feedback anyone gave us. Excluded by
        -- decision: it is not an arrival mode, so no conformed channel describes it.
        and coalesce(ticket_comment.comment_source_channel, '') != 'sample_ticket'
)

select
    'zendesk' as source_slug
    , requester_turns.comment_created_at as occurred_at
    , cast(requester_turns.comment_id as varchar) as source_record_ref
    , requester_turns.comment_plain_body as text
    , ticket.ticket_subject as title
    , cast(requester_turns.ticket_id as varchar) as conversation_ref
    , requester_turns.turn_index
    , requester_turns.turn_index = 1 as is_conversation_opening
    -- last-resort identity path: Zendesk exposes no openedx user id, only an email
    , requester_turns.comment_author_email as subject_user_ref
    , ticket.ticket_api_url as source_url
    -- An unmapped value stays null here but not downstream: generate_surrogate_key
    -- hashes the null, so it surfaces as an orphan feedback_channel_fk that fails the
    -- fact's relationships test. That is the intended loud failure, not a catch-all.
    --
    , case requester_turns.comment_source_channel
        when 'email' then 'email'
        when 'web' then 'web_form'
        when 'api' then 'api'
        when 'mobile' then 'mobile'
    end as channel_slug
    , cast(null as varchar) as courserun_readable_id
    , cast(null as varchar) as platform
    -- The Appzi URL decode is not implemented: where Appzi stores the viewed URL is
    -- unconfirmed, and guessing would populate subject_url with an untrustworthy value
    , 'unspecified' as subject_type
    , cast(null as varchar) as subject_ref
    , cast(null as varchar) as subject_url
    , ticket.ticket_satisfaction_rating_score as explicit_rating
    , requester_turns.comment_created_at as created_at
    , ticket.ticket_updated_at as updated_at
    -- carried for bridge_feedback_tag to explode; not part of the event contract
    , ticket.ticket_tags
    -- Excludes ticket_satisfaction_rating_comment and custom_fields: both carry free
    -- text with the PII Presidio strips from title/text, and neither is profiled yet
    , json_object(
        'ticket_status': ticket.ticket_status
        , 'ticket_priority': ticket.ticket_priority
        , 'ticket_due_at': ticket.ticket_due_at
        , 'brand_name': ticket.brand_name
        , 'group_name': ticket.group_name
        , 'organization_name': ticket.organization_name
    ) as source_metadata
from requester_turns
inner join ticket on requester_turns.ticket_id = ticket.ticket_id
