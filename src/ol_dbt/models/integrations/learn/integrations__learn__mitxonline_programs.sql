{#
  integrations__learn__mitxonline_programs
  Exposes MITx Online programs for MIT Learn's Trino-pull ETL.
  Contract: docs/learn_marts_contract.md
#}

with programs as (
    select * from {{ ref('int__mitxonline__programs') }}
)

, program_courses as (
    select
        reqs.program_id
        , {{ array_join('array_agg(courses.course_readable_id)', ', ') }} as program_course_ids
    from {{ ref('int__mitxonline__program_requirements') }} reqs
    inner join {{ ref('stg__mitxonline__app__postgres__courses_course') }} courses
        on reqs.course_id = courses.course_id
    group by reqs.program_id
)

select
    programs.program_readable_id                            as readable_id
    , programs.program_title                                as title
    , coalesce(
        programs.program_page_last_published_on,
        programs.program_page_first_published_on,
        {{ cast_timestamp_to_iso8601('current_timestamp') }}
    )                                                       as last_modified
    , 'mitxonline'                                          as etl_source
    , programs.program_description                          as description
    , programs.program_page_url_path                        as url
    , null                                                  as image_url
    , programs.program_is_live                              as published
    , 'mitxonline'                                          as platform
    , programs.program_type                                 as program_type
    , programs.program_certification_type                   as certification_type
    , programs.program_topics                               as topics
    , programs.program_instructors                          as instructors
    , programs.program_page_slug                            as page_slug
    , programs.program_price                                as price
    , programs.program_effort                               as effort
    , program_courses.program_course_ids                    as courses
from programs
left join program_courses on programs.program_id = program_courses.program_id
where programs.program_is_live = true
