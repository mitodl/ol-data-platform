with
    ticket_comment as (  -- noqa: PRS
        select * from {{ ref("stg__zendesk__ticket_comment") }}
    ),
    user as (select * from {{ ref("stg__zendesk__user") }})

select
    ticket_comment.comment_id,
    ticket_comment.ticket_id,
    ticket_comment.comment_type,
    ticket_comment.comment_is_public,
    ticket_comment.comment_uploads,
    user.user_name as comment_author,
    ticket_comment.audit_id,
    ticket_comment.comment_html_body,
    ticket_comment.comment_plain_body,
    ticket_comment.comment_attachments,
    ticket_comment.comment_source_channel,
    ticket_comment.comment_source_email,
    ticket_comment.comment_source_rel,
    ticket_comment.comment_via_object,
    ticket_comment.comment_unix_timestamp,
    ticket_comment.comment_created_at
from ticket_comment
left join user on ticket_comment.comment_author_user_id = user.user_id
