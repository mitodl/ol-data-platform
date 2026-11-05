with ticket_comment as (  -- noqa: PRS
   select * from {{ ref('stg__zendesk__ticket_comment') }}
)

, users as (
    select * from {{ ref('stg__zendesk__user') }}
)

-- Zendesk marks a delete only on the ticket, and a deleted ticket's comments stay in
-- staging unchanged, so they are dropped through the ticket's status.
, deleted_ticket as (
    select ticket_id from {{ ref('stg__zendesk__ticket') }}
    where ticket_status = 'deleted'
)

select
    ticket_comment.comment_id
    , ticket_comment.ticket_id
    , ticket_comment.comment_type
    , ticket_comment.comment_is_public
    , ticket_comment.comment_uploads
    -- from ticket_comment, not user: a comment whose author is missing from
    -- stg__zendesk__user must still carry its author id or the requester-vs-agent
    -- comparison silently drops the row
    , ticket_comment.comment_author_user_id
    , users.user_name as comment_author
    , users.user_email as comment_author_email
    , ticket_comment.audit_id
    , ticket_comment.comment_html_body
    , ticket_comment.comment_plain_body
    , ticket_comment.comment_attachments
    , ticket_comment.comment_source_channel
    , ticket_comment.comment_source_email
    , ticket_comment.comment_source_rel
    , ticket_comment.comment_via_object
    , ticket_comment.comment_unix_timestamp
    , ticket_comment.comment_created_at
from ticket_comment
left join users on ticket_comment.comment_author_user_id = users.user_id
left join deleted_ticket on ticket_comment.ticket_id = deleted_ticket.ticket_id
where deleted_ticket.ticket_id is null
