select
    org
    , course_id
    , user_id
    , role
from {{ source('ol_warehouse_raw_data','raw__xpro__openedx__mysql__student_courseaccessrole') }}
