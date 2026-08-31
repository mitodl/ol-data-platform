with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__courseware_studentmodule') }}
)

{{ deduplicate_raw_table(order_by='modified' , partition_columns = 'course_id, student_id, module_id') }}

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
from most_recent_source
