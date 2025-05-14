-- Program Requirement for MicroMaster
-- courses are categorized as "Core" and "Elective"
-- e.g. On production, program requirement for "Statistics and Data Science" (SDS) looks like this:
--- (program_num_required_courses = electiveset_required_number + # of Core courses)

-- program_id    course_id	   Type	      electiveset_required_number    program_num_required_courses
-----------------------------------------------------------------------------------------------------
--  4	           27	      Elective	           1                              5
--  4	           37	      Elective	           1                              5
--  4	           30	      Core                                                5
--  4	           29	      Core                                                5
--  4	           26	      Core                                                5
--  4	           28	      Core                                                5


with programs as (
    select * from {{ ref('stg__micromasters__app__postgres__courses_program') }}
)

, courses as (
    select * from {{ ref('stg__micromasters__app__postgres__courses_course') }}
)

, electiveset as (
    select * from {{ ref('stg__micromasters__app__postgres__courses_electiveset') }}
)

, electiveset_to_course as (
    select * from {{ ref('stg__micromasters__app__postgres__courses_electiveset_to_course') }}
)

, elective_courses as (
    select
        electiveset_to_course.course_id
        , electiveset.electiveset_id
        , electiveset.program_id
        , electiveset.electiveset_required_number
    from electiveset_to_course
    inner join electiveset
        on electiveset_to_course.electiveset_id = electiveset.electiveset_id
)

--- courses that are not in elective_courses are core
, core_courses as (
    select
        courses.course_id
        , courses.program_id
    from courses
    left join elective_courses
        on
            courses.program_id = elective_courses.program_id
            and courses.course_id = elective_courses.course_id
    where elective_courses.course_id is null
)

, combined_courses as (
    select
        null as electiveset_id
        , program_id
        , null as electiveset_required_number
        , course_id
        , 'Core' as programrequirement_type
    from core_courses

    union all

    select
        electiveset_id
        , program_id
        , electiveset_required_number
        , course_id
        , 'Elective' as programrequirement_type
    from elective_courses
)

select
    combined_courses.program_id
    , combined_courses.course_id
    , combined_courses.electiveset_id
    , combined_courses.programrequirement_type
    , combined_courses.electiveset_required_number
    , programs.program_num_required_courses
from combined_courses
inner join programs on combined_courses.program_id = programs.program_id
