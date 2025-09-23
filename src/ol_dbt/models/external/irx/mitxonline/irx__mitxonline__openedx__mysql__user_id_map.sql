with
    student_anonymoususerid as (
        select * from {{ source("ol_warehouse_raw_data", "raw__mitxonline__openedx__mysql__student_anonymoususerid") }}
    ),
    auth_user as (select * from {{ source("ol_warehouse_raw_data", "raw__mitxonline__openedx__mysql__auth_user") }}),
    student_courseenrollment as (
        select * from {{ source("ol_warehouse_raw_data", "raw__mitxonline__openedx__mysql__student_courseenrollment") }}
    )

select student_anonymoususerid.anonymous_user_id as hash_id, student_anonymoususerid.user_id, auth_user.username
from student_anonymoususerid
inner join auth_user on student_anonymoususerid.user_id = auth_user.id
inner join student_courseenrollment on auth_user.id = student_courseenrollment.user_id
