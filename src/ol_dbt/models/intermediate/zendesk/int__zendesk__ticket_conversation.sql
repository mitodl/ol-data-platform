-- Grain: one public comment = one turn of a ticket's conversation.
with ticket as (
    select * from {{ ref('int__zendesk__ticket') }}
)

, normalized_comment as (
    select
        *
        , {{ appzi_normalize('comment_plain_body') }} as appzi_body
    from {{ ref('int__zendesk__ticket_comment') }}
    where
        comment_is_public = true
        -- Zendesk's own demo content
        and coalesce(comment_source_channel, '') != 'sample_ticket'
)

, ticket_comment as (
    select
        *
        , {{ is_appzi_email('appzi_body') }} as is_appzi
    from normalized_comment
    where not {{ is_appzi_notice('appzi_body') }}
)

, users as (
    select * from {{ ref('stg__zendesk__user') }}
)

, comments as (
    select
        ticket_comment.comment_id
        , ticket_comment.ticket_id
        , ticket.ticket_platform
        , ticket.ticket_status
        , ticket.ticket_subject
        , ticket.group_name
        , ticket.ticket_tags
        , ticket.ticket_satisfaction_rating_score
        , ticket.ticket_created_at
        , ticket.ticket_updated_at
        , ticket_comment.comment_source_channel
        , ticket_comment.comment_created_at
        , ticket_comment.comment_author_email
        , case
            when ticket_comment.comment_author_user_id = ticket.ticket_requester_user_id
                then 'requester'
            when users.user_role in ('agent', 'admin') then 'staff'
            else 'other_end_user'
        end as comment_author_role
        -- An Appzi survey email becomes "<type>: <answer>"
        , case
            when ticket_comment.is_appzi
                then {{ appzi_feedback_text('ticket_comment.appzi_body', 'ticket_comment.comment_plain_body') }}
            else ticket_comment.comment_plain_body
        end as comment_body
        , case
            when ticket_comment.is_appzi then {{ appzi_page_url('ticket_comment.appzi_body') }}
        end as appzi_page_url
    from ticket_comment
    inner join ticket on ticket_comment.ticket_id = ticket.ticket_id
    left join users on ticket_comment.comment_author_user_id = users.user_id
)

select
    ticket_id
    , ticket_platform
    , ticket_status
    , ticket_subject
    , group_name
    , ticket_tags
    , ticket_satisfaction_rating_score
    , ticket_created_at
    , ticket_updated_at
    , comment_id
    -- comment_id breaks ties between comments that share a created_at
    , row_number() over (
        partition by ticket_id
        order by comment_created_at, comment_id
    ) as turn_index
    , comment_author_role
    , comment_author_email
    , comment_body
    , appzi_page_url
    , comment_source_channel
    , comment_created_at
from comments
