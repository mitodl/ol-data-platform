{#
  integrations__learn__xpro_programs
  Exposes xPRO programs for MIT Learn's Trino-pull ETL.
  Contract: docs/learn_marts_contract.md
#}

with programs as (
    select * from {{ ref('int__mitxpro__programs') }}
)

, program_courses as (
    select
        programs.program_id
        , array_join(array_agg(courses.course_readable_id), ', ') as program_course_ids
    from {{ ref('int__mitxpro__coursesinprogram') }} cip
    inner join {{ ref('stg__mitxpro__app__postgres__cms_coursepage') }} cp
        on cip.coursepage_wagtail_page_id = cp.wagtail_page_id
    inner join {{ ref('stg__mitxpro__app__postgres__courses_course') }} courses
        on cp.course_id = courses.course_id
    inner join {{ ref('stg__mitxpro__app__postgres__cms_programpage') }} pp
        on cip.programpage_wagtail_page_id = pp.wagtail_page_id
    inner join {{ ref('stg__mitxpro__app__postgres__courses_program') }} programs
        on pp.program_id = programs.program_id
    group by programs.program_id
)

select
    programs.program_readable_id                            as readable_id
    , programs.program_title                                as title
    , coalesce(
        programs.cms_programpage_last_published_on,
        programs.cms_programpage_first_published_on
    )                                                       as last_modified
    , 'xpro'                                                as etl_source
    , programs.cms_programpage_description                  as description
    , programs.cms_programpage_url_path                     as url
    , null                                                  as image_url
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
