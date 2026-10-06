{#
  integrations__learn__xpro_programs
  The programs xPRO's catalog API (/api/programs/) lists, with the fields MIT Learn's xPRO
  ETL (learning_resources/etl/xpro.py) reads from it, including the price and dates of the
  single run MIT Learn gives a program.
  Contract: docs/learn_marts_contract.md
#}

with programs as (
    select
        programs.program_id
        , programs.program_readable_id
        , programs.program_title
        , programs.program_is_live
        , platforms.platform_name
    from {{ ref('stg__mitxpro__app__postgres__courses_program') }} as programs
    left join {{ ref('stg__mitxpro__app__postgres__courses_platform') }} as platforms
        on programs.platform_id = platforms.platform_id
)

, pages as (
    select * from {{ ref('int__mitxpro__catalog_pages') }}
    where program_id is not null
)

, courses as (
    select
        course_id
        , program_id
        , course_readable_id
        , course_is_live
        , position_in_program
    from {{ ref('stg__mitxpro__app__postgres__courses_course') }}
    where program_id is not null
)

, catalog_runs as (
    select * from {{ ref('int__mitxpro__catalog_course_runs') }}
)

-- ProgramViewSet lists a program that has any product, active or not, and takes the price
-- from its active one
, program_products as (
    select
        program_id
        , max(case when product_is_active then product_current_price end) as price
    from {{ ref('int__mitxpro__catalog_product_prices') }}
    where program_id is not null
    group by program_id
)

, program_courses as (
    select
        program_id
        , {{ array_join('array_agg(course_readable_id order by position_in_program)', ', ') }} as courses
    from courses
    where course_is_live
    group by program_id
)

-- Program.first_unexpired_run: the earliest unexpired run of the live course in position 1
-- The API does not require that run to have a product, unlike the runs it lists under a
-- course, so neither does this.
, first_runs as (
    select
        courses.program_id
        , catalog_runs.courserun_start_on
        , catalog_runs.courserun_enrollment_start_on
        , row_number() over (
            partition by courses.program_id
            order by catalog_runs.courserun_start_on, catalog_runs.courserun_id
        ) as run_rank
    from courses
    inner join catalog_runs on courses.course_id = catalog_runs.course_id
    where
        courses.course_is_live
        and courses.position_in_program = 1
        and catalog_runs.courserun_is_unexpired
)

-- ProgramSerializer.get_end_date: the latest end of any live run of any course in the program
, last_runs as (
    select
        courses.program_id
        , max(runs.courserun_end_on) as end_date
    from courses
    inner join {{ ref('stg__mitxpro__app__postgres__courses_courserun') }} as runs
        on courses.course_id = runs.course_id
    where runs.courserun_is_live and runs.courserun_end_on is not null
    group by courses.program_id
)

-- ProgramSerializer.get_topics: the topics of every course in the program
, topics as (
    select
        courses.program_id
        , array_agg(distinct course_topics.coursetopic_name order by course_topics.coursetopic_name) as topics
    from courses
    inner join {{ ref('int__mitxpro__courses_to_topics') }} as course_topics
        on courses.course_id = course_topics.course_id
    group by courses.program_id
)

select
    programs.program_readable_id as readable_id
    , programs.program_title as title
    , coalesce(
        pages.page_last_published_on
        , pages.page_first_published_on
        , {{ cast_timestamp_to_iso8601('current_timestamp') }}
    ) as last_modified
    , 'xpro' as etl_source
    , pages.page_description as description
    , '{{ var("mitxpro_url") }}/programs/' || programs.program_readable_id || '/' as url
    , coalesce(pages.page_thumbnail_url, '{{ var("mitxpro_url") }}/static/images/mit-dome.png') as image_url
    , coalesce(program_products.price > 0, false) as published
    , programs.platform_name as platform
    , topics.topics
    , pages.page_instructors as instructors
    , program_courses.courses
    , program_products.price
    , coalesce(first_runs.courserun_start_on, first_runs.courserun_enrollment_start_on) as start_date
    , last_runs.end_date
    , first_runs.courserun_enrollment_start_on as enrollment_start
    , pages.page_format as format
    , 'dated' as availability
    , pages.page_ceus as continuing_ed_credits
    , pages.page_duration as duration
    , pages.page_min_weeks as min_weeks
    , pages.page_max_weeks as max_weeks
    , pages.page_time_commitment as time_commitment
    , pages.page_min_weekly_hours as min_weekly_hours
    , pages.page_max_weekly_hours as max_weekly_hours
from programs
inner join pages on programs.program_id = pages.program_id
inner join program_products on programs.program_id = program_products.program_id
left join program_courses on programs.program_id = program_courses.program_id
left join first_runs on programs.program_id = first_runs.program_id and first_runs.run_rank = 1
left join last_runs on programs.program_id = last_runs.program_id
left join topics on programs.program_id = topics.program_id
where programs.program_is_live and pages.page_is_live
