-- Grain: one public comment on a solved or closed MIT Learn ticket. Input to the
-- redaction asset that feeds the support chatbot's knowledge base.
with ticket as (
    select * from {{ ref('int__zendesk__ticket') }}
    where
        ticket_platform = 'mitlearn'
        and ticket_status in ('solved', 'closed')
)

, ticket_comment as (
    select * from {{ ref('int__zendesk__ticket_comment') }}
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
        -- Cut the quoted history mail clients append (Outlook, Gmail, Zendesk's
        -- reply delimiter): it repeats earlier turns and the email addresses in them.
        , {{ strip_whitespace(regexp_replace_all(
            'ticket_comment.comment_plain_body'
            , "'(?s)(\\n[ \\t]*(_{10,}[ \\t]*\\n\\s*)?From: [^\\n]*\\n\\s*(Sent|Date): "
              ~ "|\\n[ \\t]*On [^\\n]{5,200} wrote:[ \\t]*\\n"
              ~ "|##- Please type your reply above this line -##).*'"
            , "''"
        )) }} as comment_body
    from ticket_comment
    inner join ticket on ticket_comment.ticket_id = ticket.ticket_id
    left join users on ticket_comment.comment_author_user_id = users.user_id
)

select
    comments.comment_id
    , comments.ticket_id
    -- comment_id breaks ties between comments that share a created_at
    , row_number() over (
        partition by comments.ticket_id
        order by comments.comment_created_at, comments.comment_id
    ) as turn_index
    , comments.comment_author_role
    , comments.comment_body
    , comments.comment_source_channel
    , comments.comment_created_at
    , ticket.ticket_subject
    , ticket.ticket_status
    , ticket.ticket_tags
    , ticket.ticket_satisfaction_rating_score
    , ticket.group_name
    , ticket.ticket_created_at
    , ticket.ticket_updated_at
from comments
inner join ticket on comments.ticket_id = ticket.ticket_id
where comments.comment_body != ''
