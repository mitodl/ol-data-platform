-- Grain: one ticket, its public comments joined in order into one conversation text.
with ticket as (
    select * from {{ ref('int__zendesk__ticket') }}
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

, conversation as (
    select
        ticket_id
        , count(*) as comment_count
        , count_if(comment_author_role = 'staff') as staff_comment_count
        , min(comment_created_at) as first_comment_at
        , max(comment_created_at) as last_comment_at
        , {{ array_join(
            "array_agg('[' || comment_author_role || '] ' || comment_body"
            ~ " order by comment_created_at, comment_id)"
            , '\n---\n'
        ) }} as conversation_text
    from comments
    where comment_body != ''
    group by ticket_id
)

select
    ticket.ticket_id
    , ticket.ticket_platform
    , ticket.ticket_status
    , ticket.ticket_subject
    , ticket.group_name
    , ticket.brand_subdomain
    , ticket.ticket_tags
    , ticket.ticket_satisfaction_rating_score
    , conversation.comment_count
    , conversation.staff_comment_count
    , conversation.conversation_text
    , conversation.first_comment_at
    , conversation.last_comment_at
    , ticket.ticket_created_at
    , ticket.ticket_updated_at
from conversation
inner join ticket on conversation.ticket_id = ticket.ticket_id
