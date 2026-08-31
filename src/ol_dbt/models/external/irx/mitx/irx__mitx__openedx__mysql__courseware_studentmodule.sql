select
    id
    , module_type
    , module_id
    , student_id
    , state
    , grade
    , created
    , modified
    , max_grade
    , done
    , course_id
from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__courseware_studentmodule') }}
