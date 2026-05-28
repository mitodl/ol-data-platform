{#
  integrations__learn__micromasters_programs
  Exposes MicroMasters programs for MIT Learn's Trino-pull ETL.
  Contract: docs/learn_marts_contract.md
#}

with programs as (
    select * from {{ ref('int__micromasters__programs') }}
)

, program_courses as (
    select
        program_id
        , array_join(array_agg(course_readable_id order by course_position_in_program), ', ')
            as program_course_ids
    from {{ ref('int__micromasters__program_requirements') }} reqs
    inner join {{ ref('stg__micromasters__app__postgres__courses_course') }} courses
        on reqs.course_id = courses.course_id
    group by program_id
)

select
    -- MicroMasters uses a numeric program_id; build a stable readable_id
    cast(programs.program_id as varchar)                    as readable_id
    , programs.program_title                                as title
    -- MicroMasters staging has no last_modified; use current_timestamp as a
    -- conservative fallback so the NOT NULL contract is satisfied.
    , current_timestamp                                     as last_modified
    , 'micromasters'                                        as etl_source
    , programs.program_description                          as description
    , null                                                  as url
    , null                                                  as image_url
    , programs.program_is_live                              as published
    , 'micromasters'                                        as platform
    , program_courses.program_course_ids                    as courses
from programs
left join program_courses on programs.program_id = program_courses.program_id
where programs.program_is_live = true
