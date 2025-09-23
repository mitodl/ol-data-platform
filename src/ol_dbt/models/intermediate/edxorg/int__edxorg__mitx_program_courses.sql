with
    programs as (select * from {{ ref("stg__edxorg__s3__programs") }}),
    program_courses as (select * from {{ ref("stg__edxorg__s3__program_courses") }}),
    micromasters_programs as (select * from {{ ref("int__mitx__programs") }} where is_micromasters_program = true)

select
    programs.program_uuid,
    programs.program_title,
    programs.program_type,
    programs.program_status,
    program_courses.course_readable_id,
    program_courses.course_title,
    program_courses.course_type,
    coalesce(micromasters_programs.program_title, programs.program_title) as program_name
from program_courses
inner join programs on program_courses.program_uuid = programs.program_uuid
left join
    micromasters_programs
    on (
        programs.program_title like micromasters_programs.program_title || '%'
        or programs.program_title like '%' || micromasters_programs.program_title
    )
where programs.program_organization = 'MITx'
