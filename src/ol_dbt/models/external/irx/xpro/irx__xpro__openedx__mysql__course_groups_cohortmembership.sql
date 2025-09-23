with
    cm as (
        select * from {{ source("ol_warehouse_raw_data", "raw__xpro__openedx__mysql__course_groups_cohortmembership") }}
    ),
    cug as (
        select * from {{ source("ol_warehouse_raw_data", "raw__xpro__openedx__mysql__course_groups_courseusergroup") }}
    )

select cm.user_id, cm.course_id, cug.group_type, cug.name
from cm
inner join cug on cm.course_user_group_id = cug.id
