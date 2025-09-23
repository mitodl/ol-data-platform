with
    dccr as (
        select * from {{ source("ol_warehouse_raw_data", "raw__mitx__openedx__mysql__django_comment_client_role") }}
    ),
    dccru as (
        select *
        from {{ source("ol_warehouse_raw_data", "raw__mitx__openedx__mysql__django_comment_client_role_users") }}
    )

select dccr.course_id, dccru.user_id, dccr.name
from dccr
inner join dccru on dccr.id = dccru.role_id
