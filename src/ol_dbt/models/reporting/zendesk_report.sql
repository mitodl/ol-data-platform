with zendesk__ticket as (
    select * from {{ ref("int__zendesk__ticket") }}
)

select 
    ticket_created_at
    , ticket_requester
    , ticket_subject
    , ticket_description
    , ticket_id
    , brand_name
    , group_name
from zendesk__ticket