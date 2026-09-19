{#
  The instructors of each edX.org program MIT Learn lists: the staff of the best run
  (int__edxorg__mitx_learn_course_best_runs) of every course in the program that MIT
  Learn publishes, one row per distinct instructor, positioned by last name as MIT
  Learn's legacy ETL sorted them.
#}

with program_courses as (
    select *
    from {{ ref('stg__edxorg__s3__program_courses') }}
    where program_course_retrieved_at = (
        select max(program_course_retrieved_at) from {{ ref('stg__edxorg__s3__program_courses') }}
    )
)

, staff as (
    select distinct
        program_courses.program_uuid
        , nullif(trim({{ json_extract_scalar('instructor.person', "'$.first_name'") }}), '') as first_name
        , nullif(trim({{ json_extract_scalar('instructor.person', "'$.last_name'") }}), '') as last_name
    from program_courses
    inner join {{ ref('int__edxorg__mitx_learn_programs') }} as programs
        on program_courses.program_uuid = programs.readable_id
    inner join {{ ref('int__edxorg__mitx_learn_course_best_runs') }} as courses
        on program_courses.course_key = courses.course_readable_id
    cross join {{ unnest_json_array('courses.best_run_instructors_json', 'instructor', 'person') }}
    where courses.is_published
)

, named_staff as (
    select
        program_uuid
        , first_name
        , last_name
        , trim(concat(coalesce(first_name, ''), ' ', coalesce(last_name, ''))) as full_name
    from staff
)

select
    program_uuid
    , first_name
    , last_name
    , full_name
    , row_number() over (
        partition by program_uuid order by coalesce(last_name, full_name), full_name
    ) as instructor_position
from named_staff
where full_name != ''
