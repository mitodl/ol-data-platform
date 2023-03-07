{{ config(materialized='view') }}

with edx_enrollments as (
    select *
    from {{ ref('int__edxorg__mitx_courserun_enrollments') }}
)


, edx_runs as (
    select *
    from {{ ref('int__edxorg__mitx_courseruns') }}
    where micromasters_program_id is not null
)

, edx_users as (
    select *
    from {{ ref('int__edxorg__mitx_users') }}
)

, micromasters_programs as (
    select *
    from {{ ref('int__micromasters__programs') }}
)

select
    micromasters_programs.program_title
    , edx_enrollments.user_id
    , edx_enrollments.user_username
    , edx_users.user_country
    , edx_users.user_email
    , edx_enrollments.courserunenrollment_enrollment_mode
    , edx_enrollments.courserun_readable_id
    , edx_enrollments.courserunenrollment_created_on
    , edx_enrollments.courserunenrollment_is_active
    , edx_enrollments.courserun_title
    , '{{ var("edxorg") }}' as platform
from edx_enrollments
inner join edx_runs
    on edx_enrollments.courserun_readable_id = edx_runs.courserun_readable_id
inner join edx_users on edx_users.user_id = edx_enrollments.user_id
inner join micromasters_programs on micromasters_programs.program_id = edx_runs.micromasters_program_id
