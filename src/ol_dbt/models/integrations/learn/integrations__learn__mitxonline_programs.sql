{#
  integrations__learn__mitxonline_programs
  The programs MITx Online's catalog API (/api/v2/programs/?live=true) lists, live and not
  B2B-only, with the fields MIT Learn's MITx Online ETL (learning_resources/etl/mitxonline.py)
  reads from it, including the dates and prices of the single run MIT Learn gives a program.
  Its courses and child programs are in integrations__learn__mitxonline_program_children.
  Contract: docs/learn_marts_contract.md
#}

with programs as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_program') }}
    where program_is_live and not coalesce(program_is_b2b_only, false)
)

, pages as (
    select
        *
        -- MITx Online's pages API lists a page to MIT Learn only when it is live and in the
        -- Learn catalog, and MIT Learn's ETL reads full_description's sections from there
        , page_is_live and page_is_in_learn_catalog as page_is_listed_to_learn
    from {{ ref('int__mitxonline__catalog_pages') }}
    where program_id is not null
)

, program_modes as (
    select
        program_modes.program_id
        , max(case when modes.enrollmentmode_slug = 'verified' then 1 else 0 end) = 1 as has_verified_mode
        , max(case when modes.enrollmentmode_slug = 'audit' then 1 else 0 end) = 1 as has_audit_mode
    from {{ ref('stg__mitxonline__app__postgres__courses_program_enrollment_modes') }} as program_modes
    inner join {{ ref('stg__mitxonline__app__postgres__courses_enrollmentmode') }} as modes
        on program_modes.enrollmentmode_id = modes.enrollmentmode_id
    group by program_modes.program_id
)

, program_courses as (
    select distinct
        program_id
        , course_id
    from {{ ref('stg__mitxonline__app__postgres__courses_programrequirement') }}
    where programrequirement_node_type = 'course' and course_id is not null
)

-- Program.next_starting_run: the earliest future start of a live run, source runs excluded,
-- of any course in the program's requirements. ProgramSerializer.get_start_date falls back
-- to the program's own start date.
, next_starts as (
    select
        program_courses.program_id
        , min(runs.courserun_start_on) as next_start_on
    from program_courses
    inner join {{ ref('stg__mitxonline__app__postgres__courses_courserun') }} as runs
        on program_courses.course_id = runs.course_id
    where
        runs.courserun_is_live
        and not coalesce(runs.courserun_is_source_run, false)
        and coalesce(runs.courserun_tag, '') != 'SOURCE'
        and {{ from_iso8601_timestamp('runs.courserun_start_on') }} > {{ current_timestamptz() }}
    group by program_courses.program_id
)

-- ProgramSerializer.get_topics: the page topics of the program's live courses, without the
-- parent topics a course lists
, topic_names as (
    select distinct
        program_courses.program_id
        , course_topics.coursetopic_name
    from program_courses
    inner join {{ ref('stg__mitxonline__app__postgres__courses_course') }} as courses
        on program_courses.course_id = courses.course_id
    inner join {{ ref('int__mitxonline__course_to_topics') }} as course_topics
        on program_courses.course_id = course_topics.course_id
    where courses.course_is_live
)

, topics as (
    select
        program_id
        , array_agg(coursetopic_name order by coursetopic_name) as topics
    from topic_names
    group by program_id
)

, departments as (
    select
        program_departments.program_id
        , array_agg(distinct departments.coursedepartment_name order by departments.coursedepartment_name) as departments
    from {{ ref('stg__mitxonline__app__postgres__courses_program_to_department') }} as program_departments
    inner join {{ ref('stg__mitxonline__app__postgres__courses_department') }} as departments
        on program_departments.coursedepartment_id = departments.coursedepartment_id
    group by program_departments.program_id
)

, priced as (
    select
        programs.program_id
        , pages.page_min_price
        , pages.page_max_price
        , coalesce(program_modes.has_verified_mode, false) as has_verified_mode
        , coalesce(program_modes.has_audit_mode, false) as has_audit_mode
        , (coalesce(pages.page_min_price, 0) != 0 or coalesce(pages.page_max_price, 0) != 0) as is_priced
    from programs
    left join pages on programs.program_id = pages.program_id
    left join program_modes on programs.program_id = program_modes.program_id
)

-- parse_prices, as for a course run: the verified mode adds the page's min and max price,
-- the audit mode adds zero, and a program with no page price or neither mode is free
, price_rows as (
    select
        program_id
        , cast(0 as decimal(10, 2)) as price
    from priced
    where not is_priced or has_audit_mode or not has_verified_mode
    union
    select
        program_id
        , cast(coalesce(page_min_price, 0) as decimal(10, 2)) as price
    from priced
    where is_priced and has_verified_mode
    union
    select
        program_id
        , cast(coalesce(page_max_price, 0) as decimal(10, 2)) as price
    from priced
    where is_priced and has_verified_mode
)

, prices as (
    select
        program_id
        , array_agg(price order by price) as prices
    from price_rows
    group by program_id
)

select
    programs.program_readable_id as readable_id
    , programs.program_title as title
    , coalesce(
        pages.page_last_published_on
        , pages.page_first_published_on
        , {{ cast_timestamp_to_iso8601(current_timestamptz()) }}
    ) as last_modified
    , 'mitxonline' as etl_source
    , 'mitxonline' as platform
    , pages.page_description as description
    , coalesce(pages.page_is_listed_to_learn, false) as has_catalog_page
    , case when pages.page_is_listed_to_learn then pages.page_about end as about
    , case when pages.page_is_listed_to_learn then pages.page_what_you_learn end as what_you_learn
    , case when pages.page_is_listed_to_learn then pages.page_prerequisites end as prerequisites
    -- The pages API lists a page without FAQs with an empty array
    , case when pages.page_is_listed_to_learn then coalesce(pages.page_faqs, '[]') end as faqs
    , pages.page_feature_image_src as image_url
    , pages.wagtail_page_id is not null as has_page
    , (coalesce(pages.page_is_live, false) and coalesce(pages.page_is_in_learn_catalog, false)) as published
    , programs.program_display_mode as display_mode
    , topics.topics
    , departments.departments
    , (pages.wagtail_page_id is not null and priced.has_verified_mode) as certification
    , programs.program_certification_type as certification_type
    , programs.program_availability as availability
    , coalesce(next_starts.next_start_on, programs.program_start_on, programs.program_enrollment_start_on)
        as start_date
    , programs.program_end_on as end_date
    , programs.program_enrollment_start_on as enrollment_start
    , programs.program_enrollment_end_on as enrollment_end
    , prices.prices
    , pages.page_length as duration
    , pages.page_min_weeks as min_weeks
    , pages.page_max_weeks as max_weeks
    , pages.page_effort as time_commitment
    , pages.page_min_weekly_hours as min_weekly_hours
    , pages.page_max_weekly_hours as max_weekly_hours
    , cast(pages.page_min_price as decimal(10, 2)) as min_price
    , cast(pages.page_max_price as decimal(10, 2)) as max_price
from programs
inner join priced on programs.program_id = priced.program_id
inner join prices on programs.program_id = prices.program_id
left join pages on programs.program_id = pages.program_id
left join next_starts on programs.program_id = next_starts.program_id
left join topics on programs.program_id = topics.program_id
left join departments on programs.program_id = departments.program_id
