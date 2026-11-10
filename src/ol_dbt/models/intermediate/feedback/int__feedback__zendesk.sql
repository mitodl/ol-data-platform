-- Grain: one public, requester-authored comment = one turn.
with requester_turns as (
    select
        comment_id
        , ticket_id
        , comment_body
        , appzi_page_url
        , comment_author_email
        , comment_source_channel
        , comment_created_at
        -- Counts the requester's turns only, so it differs from the conversation's turn_index
        , row_number() over (
            partition by ticket_id
            order by comment_created_at nulls last, comment_id nulls last
        ) as turn_index
    from {{ ref('int__zendesk__ticket_conversation') }}
    where comment_author_role = 'requester'
)

, ticket as (
    select * from {{ ref('int__zendesk__ticket') }}
)

select
    'zendesk' as source_slug
    , requester_turns.comment_created_at as occurred_at
    , cast(requester_turns.comment_id as varchar) as source_record_ref
    , requester_turns.comment_body as text
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
    , cast(null as varchar) as block_id
    , ticket.ticket_platform as platform
    , case when requester_turns.appzi_page_url is not null then 'page_url' else 'unspecified' end as subject_type
    , cast(null as varchar) as subject_ref
    , requester_turns.appzi_page_url as subject_url
    , ticket.ticket_satisfaction_rating_score as explicit_rating
    , requester_turns.comment_created_at as created_at
    , ticket.ticket_updated_at as updated_at
    -- carried for bridge_feedback_tag to explode; not part of the event contract
    , ticket.ticket_tags
    -- Excludes ticket_satisfaction_rating_comment and custom_fields: both carry free
    -- text with the PII Presidio strips from title/text, and neither is profiled yet
    , {{ json_object_from_pairs([
        ['ticket_status', "ticket.ticket_status"]
        , ['ticket_priority', "ticket.ticket_priority"]
        , ['ticket_due_at', "cast(ticket.ticket_due_at as varchar)"]
        , ['brand_name', "ticket.brand_name"]
        , ['brand_subdomain', "ticket.brand_subdomain"]
        , ['group_name', "ticket.group_name"]
        , ['organization_name', "ticket.organization_name"]
    ]) }} as source_metadata
from requester_turns
inner join ticket on requester_turns.ticket_id = ticket.ticket_id
