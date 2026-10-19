{#
  integrations__learn__mitxonline_courses
  The courses MITx Online's catalog API for MIT Learn (/api/internal/courses/) lists, with
  the fields MIT Learn's MITx Online ETL (learning_resources/etl/mitxonline.py) reads from
  it or derives from its runs. Runs are in integrations__learn__mitxonline_runs.
  Contract: docs/learn_marts_contract.md
#}

with courses as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_course') }}
    -- MIT Learn's ETL drops these before loading anything
    where upper(course_title) not like '%PROCTORED EXAM%'
)

, pages as (
    select
        *
        -- MITx Online's pages API lists a page to MIT Learn only when it is live and in the
        -- Learn catalog, and MIT Learn's ETL reads full_description's sections from there
        , page_is_live and page_is_in_learn_catalog as page_is_listed_to_learn
    from {{ ref('int__mitxonline__catalog_pages') }}
    where course_id is not null
)

, runs as (
    select * from {{ ref('integrations__learn__mitxonline_runs') }}
)

, catalog_runs as (
    select * from {{ ref('int__mitxonline__catalog_course_runs') }}
)

, run_rollup as (
    select
        readable_id
        , max(case when published then 1 else 0 end) = 1 as has_published_run
        , max(case when published and status = 'Current' and has_verified_mode then 1 else 0 end) = 1
            as certification
        , array_agg(distinct pace order by pace) as pace
    from runs
    group by readable_id
)

-- CourseSerializer.get_availability on the internal endpoint: dated when any run, source
-- runs included, is enrollable and not self-paced
, dated_courses as (
    select distinct course_id
    from catalog_runs
    where courserun_is_enrollable and not coalesce(courserun_is_self_paced, false)
)

-- The course number of each run, latest run first and lowest run id first among runs with the
-- same start. The first number is the primary one and the rest are cross-listed.
, ranked_runs as (
    select
        course_id
        , courserun_course_number
        , row_number() over (
            partition by course_id
            order by coalesce(courserun_start_on, courserun_enrollment_start_on, '') desc, courserun_id
        ) as recency_rank
    from catalog_runs
)

, course_number_ranks as (
    select
        course_id
        , courserun_course_number
        , min(recency_rank) as recency_rank
    from ranked_runs
    where courserun_course_number is not null and courserun_course_number != ''
    group by course_id, courserun_course_number
)

, course_numbers as (
    select
        course_id
        , array_agg(courserun_course_number order by recency_rank) as course_numbers
    from course_number_ranks
    group by course_id
)

-- CourseSerializer.get_topics: the page's topics and the parents of those topics
, topic_names as (
    select
        course_id
        , coursetopic_name
    from {{ ref('int__mitxonline__course_to_topics') }}
    union
    select
        course_topics.course_id
        , parents.coursetopic_name
    from {{ ref('int__mitxonline__course_to_topics') }} as course_topics
    inner join {{ ref('stg__mitxonline__app__postgres__courses_coursetopic') }} as parents
        on course_topics.coursetopic_parent_id = parents.coursetopic_id
)

, topics as (
    select
        course_id
        , array_agg(coursetopic_name order by coursetopic_name) as topics
    from topic_names
    group by course_id
)

, departments as (
    select
        course_departments.course_id
        , array_agg(distinct departments.coursedepartment_name order by departments.coursedepartment_name) as departments
    from {{ ref('stg__mitxonline__app__postgres__courses_course_to_department') }} as course_departments
    inner join {{ ref('stg__mitxonline__app__postgres__courses_department') }} as departments
        on course_departments.coursedepartment_id = departments.coursedepartment_id
    group by course_departments.course_id
)

-- CourseSerializer.get_certificate_type reads the first of the course's programs that are
-- live with a live page. The API does not order them, so the lowest program id stands in
-- for first.
, program_certificate_types as (
    select
        requirements.course_id
        , programs.program_certification_type
        , row_number() over (partition by requirements.course_id order by programs.program_id) as program_rank
    from {{ ref('stg__mitxonline__app__postgres__courses_programrequirement') }} as requirements
    inner join {{ ref('stg__mitxonline__app__postgres__courses_program') }} as programs
        on requirements.program_id = programs.program_id
    inner join {{ ref('int__mitxonline__catalog_pages') }} as program_pages
        on programs.program_id = program_pages.program_id
    where
        requirements.programrequirement_node_type = 'course'
        and requirements.course_id is not null
        and programs.program_is_live
        and program_pages.page_is_live
)

select
    courses.course_readable_id as readable_id
    , courses.course_title as title
    , coalesce(
        pages.page_last_published_on
        , pages.page_first_published_on
        , {{ cast_timestamp_to_iso8601('current_timestamp') }}
    ) as last_modified
    , 'mitxonline' as etl_source
    , 'mitxonline' as platform
    , pages.page_description as description
    , coalesce(pages.page_is_listed_to_learn, false) as has_catalog_page
    , case when pages.page_is_listed_to_learn then pages.page_about end as about
    , case when pages.page_is_listed_to_learn then pages.page_what_you_learn end as what_you_learn
    , case when pages.page_is_listed_to_learn then pages.page_prerequisites end as prerequisites
    , pages.page_feature_image_src as image_url
    , pages.wagtail_page_id is not null as has_page
    , (
        coalesce(pages.page_is_live, false)
        and coalesce(run_rollup.has_published_run, false)
        and coalesce(pages.page_is_in_learn_catalog, false)
    ) as published
    , coalesce(pages.page_is_in_learn_catalog, false) as include_in_learn_catalog
    , coalesce(pages.page_ingests_content_files_for_ai, false) as ingest_content_files_for_ai
    , topics.topics
    , departments.departments
    , course_numbers.course_numbers
    , coalesce(run_rollup.certification, false) as certification
    , coalesce(program_certificate_types.program_certification_type, 'Certificate of Completion')
        as certification_type
    , if(dated_courses.course_id is not null, 'dated', 'anytime') as availability
    , run_rollup.pace
    , pages.page_length as duration
    , pages.page_min_weeks as min_weeks
    , pages.page_max_weeks as max_weeks
    , pages.page_effort as time_commitment
    , pages.page_min_weekly_hours as min_weekly_hours
    , pages.page_max_weekly_hours as max_weekly_hours
    , cast(pages.page_min_price as decimal(10, 2)) as min_price
    , cast(pages.page_max_price as decimal(10, 2)) as max_price
from courses
left join pages on courses.course_id = pages.course_id
left join run_rollup on courses.course_readable_id = run_rollup.readable_id
left join dated_courses on courses.course_id = dated_courses.course_id
left join course_numbers on courses.course_id = course_numbers.course_id
left join topics on courses.course_id = topics.course_id
left join departments on courses.course_id = departments.course_id
left join program_certificate_types
    on courses.course_id = program_certificate_types.course_id and program_certificate_types.program_rank = 1
