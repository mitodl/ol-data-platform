with
    auth_userprofile as (
        select * from {{ source("ol_warehouse_raw_data", "raw__mitx__openedx__mysql__auth_userprofile") }}
    ),
    student_courseenrollment as (
        select * from {{ source("ol_warehouse_raw_data", "raw__mitx__openedx__mysql__student_courseenrollment") }}
    )

select
    auth_userprofile.id,
    auth_userprofile.user_id,
    auth_userprofile.name,
    auth_userprofile.language,
    auth_userprofile.location,
    auth_userprofile.meta,
    auth_userprofile.courseware,
    auth_userprofile.gender,
    auth_userprofile.mailing_address,
    auth_userprofile.year_of_birth,
    auth_userprofile.level_of_education,
    auth_userprofile.goals,
    auth_userprofile.country,
    auth_userprofile.city,
    auth_userprofile.bio,
    auth_userprofile.profile_image_uploaded_at,
    auth_userprofile.state
from auth_userprofile
inner join student_courseenrollment on auth_userprofile.user_id = student_courseenrollment.user_id
