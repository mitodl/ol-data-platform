-- Grain: one public comment = one turn of a ticket's conversation.
with ticket as (
    select * from {{ ref('int__zendesk__ticket') }}
)

, ticket_comment as (
    select
        *
        , {{ appzi_normalize('comment_plain_body') }} as appzi_body
    from {{ ref('int__zendesk__ticket_comment') }}
    where
        comment_is_public = true
        and coalesce(comment_source_channel, '') != 'sample_ticket'
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
        , case
            when ticket_comment.comment_author_user_id = ticket.ticket_requester_user_id
                then 'requester'
            -- Some support staff reply from accounts whose role is end-user
            when users.user_role in ('agent', 'admin') or users.user_email like '%@mit.edu'
                then 'staff'
            else 'other_end_user'
        end as comment_author_role
        , {{ zendesk_comment_text(
            'ticket_comment.appzi_body', 'ticket_comment.comment_plain_body'
        ) }} as comment_body
    from ticket_comment
    inner join ticket on ticket_comment.ticket_id = ticket.ticket_id
    left join users on ticket_comment.comment_author_user_id = users.user_id
    where not {{ is_appzi_notice('ticket_comment.appzi_body') }}
)

select
    comments.ticket_id
    , comments.ticket_platform
    , comments.ticket_status
    , comments.ticket_subject
    , comments.group_name
    , comments.ticket_tags
    , comments.ticket_satisfaction_rating_score
    , comments.ticket_created_at
    , comments.ticket_updated_at
    , comments.comment_id
    -- comment_id breaks ties between comments that share a created_at
    , row_number() over (
        partition by comments.ticket_id
        order by comments.comment_created_at, comments.comment_id
    ) as turn_index
    , comments.comment_author_role
    , comments.comment_body
    , comments.comment_source_channel
    , comments.comment_created_at
from comments
where comments.comment_body != ''
