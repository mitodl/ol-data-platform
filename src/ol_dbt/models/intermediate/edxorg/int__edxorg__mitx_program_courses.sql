with programs as (
    select * from {{ ref('stg__edxorg__s3__programs') }}
)

, program_courses as (
    select * from {{ ref('stg__edxorg__s3__program_courses') }}
)

, micromasters_programs as (
    select * from {{ ref('int__micromasters__programs') }}
)

, mitx_courses as (
    select * from {{ ref('int__mitx__courses') }}
)

-- int__mitx__courses keys a course by the course number of its edX.org runs, which drops
-- the suffix of a course edX.org re-created, e.g. MITx+16.101x_2 runs as 16.101x
, edxorg_course_numbers as (
    select distinct
        course_readable_id
        , course_number
    from {{ ref('int__edxorg__mitx_courseruns') }}
    where course_number is not null
)

select
    programs.program_uuid
    , programs.program_title
    , programs.program_type
    , programs.program_status
    , program_courses.course_readable_id
    -- The key learners' course enrollments carry: the MITx Online course key when the
    -- course moved there, else its edX.org key. A course whose runs carry no course number
    -- keeps the programs API key ({org}+{course}), which dim_course keys its edX.org row by.
    , coalesce(mitx_courses.course_readable_id, program_courses.course_key) as mitx_course_readable_id
    , program_courses.course_title
    , program_courses.course_type
    , coalesce(micromasters_programs.program_title, programs.program_title) as program_name
from program_courses
inner join programs
    on program_courses.program_uuid = programs.program_uuid
left join edxorg_course_numbers
    on program_courses.course_key = edxorg_course_numbers.course_readable_id
left join mitx_courses
    on edxorg_course_numbers.course_number = mitx_courses.course_number
left join micromasters_programs
    on (
        programs.program_title like {{ dbt.concat(["micromasters_programs.program_title", "'%'"]) }}
        or programs.program_title like {{ dbt.concat(["'%'", "micromasters_programs.program_title"]) }}
    )
where programs.program_organization = 'MITx'
