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
-- edxapp compares course ids under a case-insensitive collation, and some
-- organizationcourse rows spell the run differently from the role
-- (course-v1:MITx+20.309r_8+2024_fall vs. 2024_Fall).
left join organizationcourse on lower(dccr.course_id) = lower(organizationcourse.course_id)
left join organization on organizationcourse.organization_id = organization.id
