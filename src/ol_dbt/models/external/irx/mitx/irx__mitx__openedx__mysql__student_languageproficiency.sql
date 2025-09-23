with
    student_languageproficiency as (
        select * from {{ source("ol_warehouse_raw_data", "raw__mitx__openedx__mysql__student_languageproficiency") }}
    ),
    auth_userprofile as (
        select * from {{ source("ol_warehouse_raw_data", "raw__mitx__openedx__mysql__auth_userprofile") }}
    ),
    student_courseenrollment as (
        select * from {{ source("ol_warehouse_raw_data", "raw__mitx__openedx__mysql__student_courseenrollment") }}
    )

select student_languageproficiency.user_profile_id, student_languageproficiency.id, student_languageproficiency.code
from student_languageproficiency
inner join auth_userprofile on student_languageproficiency.user_profile_id = auth_userprofile.user_id
inner join student_courseenrollment on auth_userprofile.user_id = student_courseenrollment.user_id
