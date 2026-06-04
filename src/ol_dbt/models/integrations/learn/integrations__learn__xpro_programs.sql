{#
  integrations__learn__xpro_programs
  Exposes xPRO programs for MIT Learn's Trino-pull ETL.
  Contract: docs/learn_marts_contract.md

  Program-course membership is sourced from int__mitxpro__courses, which
  carries program_id directly, avoiding a multi-hop join through Wagtail
  CMS page tables.
#}

with programs as (
    select * from {{ ref('int__mitxpro__programs') }}
)

, program_courses as (
    select
        program_id
        , {{ array_join('array_agg(course_readable_id)', ', ') }} as program_course_ids
    from {{ ref('int__mitxpro__courses') }}
    where program_id is not null
    group by program_id
)

select
    programs.program_readable_id                            as readable_id
    , programs.program_title                                as title
    , coalesce(
        programs.cms_programpage_last_published_on,
        programs.cms_programpage_first_published_on,
        {{ cast_timestamp_to_iso8601('current_timestamp') }}
    )                                                       as last_modified
    , 'xpro'                                                as etl_source
    , programs.cms_programpage_description                  as description
    , programs.cms_programpage_url_path                     as url
    , programs.cms_programpage_image_url                    as image_url
    , programs.program_is_live                              as published
    , programs.platform_name                                as platform
    , programs.cms_programpage_slug                         as page_slug
    , programs.program_topics                               as topics
    , programs.program_instructors                          as instructors
    , programs.cms_programpage_duration                     as length
    , programs.cms_programpage_time_commitment              as effort
    , programs.cms_programpage_format                       as format
    , program_courses.program_course_ids                    as courses
from programs
left join program_courses on programs.program_id = program_courses.program_id
where programs.program_is_live = true
