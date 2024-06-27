with courserun_grades as (
    select * from {{ ref('int__edxorg__mitx_courserun_grades') }}
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
    , courserun_grades.user_username as user_edxorg_username
    , courserun_grades.courserungrade_is_passing
    , courserun_grades.courserungrade_user_grade
from courserun_grades
inner join courseruns on courserun_grades.courserun_readable_id = courseruns.courserun_readable_id
inner join programs on courseruns.micromasters_program_id = programs.micromasters_program_id
where programs.is_dedp_program = false
