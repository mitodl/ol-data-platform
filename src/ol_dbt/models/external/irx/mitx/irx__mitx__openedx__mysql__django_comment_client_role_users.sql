with dccr as (
    select *
    from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__django_comment_client_role') }}
)

, dccru as (
    select *
    from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__django_comment_client_role_users') }}
)

, organizationcourse as (
    select *
    from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__organizations_organizationcourse') }}
)

, organization as (
    select *
    from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__organizations_organization') }}
)

select
    dccr.course_id
    , dccru.user_id
    , dccr.name
    , dccru.id
    , organization.name as org
from dccr
inner join dccru on dccr.id = dccru.role_id
left join organizationcourse on dccr.course_id = organizationcourse.course_id
left join organization on organizationcourse.organization_id = organization.id
