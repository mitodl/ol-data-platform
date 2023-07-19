with courserun_grades as (
    select * from {{ ref('int__edxorg__mitx_courserun_grades') }}
)

, edxorg_users as (
    select * from {{ ref('int__edxorg__mitx_users') }}
)

, micromasters_users as (
    select * from {{ ref('__micromasters__users') }}
)

, courseruns as (
    select * from {{ ref('int__edxorg__mitx_courseruns') }}
)

, programs as (
    select * from {{ ref('int__mitx__programs') }}
)

select
    programs.program_title
    , programs.mitxonline_program_id
    , programs.micromasters_program_id
    , courseruns.courserun_title
    , courseruns.courserun_readable_id
    , '{{ var("edxorg") }}' as courserun_platform
    , courseruns.course_number
    , edxorg_users.user_username as user_edxorg_username
    , micromasters_users.user_mitxonline_username
    , edxorg_users.user_full_name
    , edxorg_users.user_country
    , edxorg_users.user_email
    , courserun_grades.courserungrade_is_passing
    , courserun_grades.courserungrade_user_grade
from courserun_grades
inner join courseruns on courserun_grades.courserun_readable_id = courseruns.courserun_readable_id
inner join programs on courseruns.micromasters_program_id = programs.micromasters_program_id
inner join edxorg_users on courserun_grades.user_id = edxorg_users.user_id
left join micromasters_users on edxorg_users.user_username = micromasters_users.user_edxorg_username
where programs.is_dedp_program = false
