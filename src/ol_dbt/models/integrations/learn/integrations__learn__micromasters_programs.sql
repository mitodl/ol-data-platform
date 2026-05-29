{#
  integrations__learn__micromasters_programs
  Exposes MicroMasters programs for MIT Learn's Trino-pull ETL.
  Contract: docs/learn_marts_contract.md
#}

with programs as (
    select * from {{ ref('int__micromasters__programs') }}
)

-- Join staging directly to access program_updated_on, which int__micromasters__programs
-- does not expose but is required for the last_modified contract column.
, stg_programs as (
    select
        program_id
        , program_updated_on
    from {{ ref('stg__micromasters__app__postgres__courses_program') }}
)

, program_courses as (
    select
        reqs.program_id
        , {{ array_join('array_agg(courses.course_edx_key order by reqs.course_position_in_program)', ', ') }}
            as program_course_ids
    from {{ ref('int__micromasters__program_requirements') }} reqs
    inner join {{ ref('stg__micromasters__app__postgres__courses_course') }} courses
        on reqs.course_id = courses.course_id
    group by reqs.program_id
)

select
    cast(programs.program_id as varchar)                    as readable_id
    , programs.program_title                                as title
    , stg_programs.program_updated_on                       as last_modified
    , 'micromasters'                                        as etl_source
    , programs.program_description                          as description
    , null                                                  as url
    , null                                                  as image_url
    , programs.program_is_live                              as published
    , 'micromasters'                                        as platform
    , program_courses.program_course_ids                    as courses
from programs
inner join stg_programs on programs.program_id = stg_programs.program_id
left join program_courses on programs.program_id = program_courses.program_id
where programs.program_is_live = true
