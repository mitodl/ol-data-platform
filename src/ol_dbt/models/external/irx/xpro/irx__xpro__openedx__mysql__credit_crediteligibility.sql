with
    ce as (select * from {{ source("ol_warehouse_raw_data", "raw__xpro__openedx__mysql__credit_crediteligibility") }}),
    cc as (select * from {{ source("ol_warehouse_raw_data", "raw__xpro__openedx__mysql__credit_creditcourse") }})

select ce.id, ce.created, ce.modified, ce.username, ce.deadline, cc.course_key as course_id
from ce
inner join cc on ce.course_id = cc.id
order by ce.username
