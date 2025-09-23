select course_id, mode, id, is_active
from {{ source("ol_warehouse_raw_data", "raw__xpro__openedx__mysql__student_courseenrollment") }}
