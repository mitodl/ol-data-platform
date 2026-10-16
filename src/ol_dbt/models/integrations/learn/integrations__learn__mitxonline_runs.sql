{#
  integrations__learn__mitxonline_runs
  Every run of the courses in integrations__learn__mitxonline_courses, as MITx Online's
  catalog API for MIT Learn lists them under a course, with what MIT Learn's MITx Online ETL
  derives per run: whether it is published, a variant, current, and its prices.
  Contract: docs/learn_marts_contract.md
#}

with catalog_runs as (
    select * from {{ ref('int__mitxonline__catalog_course_runs') }}
)

, courses as (
    select
        course_id
        , course_readable_id
    from {{ ref('stg__mitxonline__app__postgres__courses_course') }}
    where upper(course_title) not like '%PROCTORED EXAM%'
)

, pages as (
    select * from {{ ref('int__mitxonline__catalog_pages') }}
    where course_id is not null
)

, runs as (
    select
        catalog_runs.*
        , courses.course_readable_id
        , pages.page_instructors
        , pages.page_min_price
        , pages.page_max_price
        , pages.wagtail_page_id is not null as has_page
        , coalesce(pages.page_is_live, false) as page_is_live
        -- parse_prices: a run is free unless it is fully enrollable and the course page has
        -- a price
        , (
            catalog_runs.courserun_is_fully_enrollable
            and (coalesce(pages.page_min_price, 0) != 0 or coalesce(pages.page_max_price, 0) != 0)
        ) as is_priced
    from catalog_runs
    inner join courses on catalog_runs.course_id = courses.course_id
    left join pages on catalog_runs.course_id = pages.course_id
)

-- parse_prices: the verified mode adds the page's min and max price and the audit mode adds
-- zero. With neither mode the run is free.
, price_rows as (
    select
        courserun_id
        , cast(0 as decimal(10, 2)) as price
    from runs
    where not is_priced or courserun_has_audit_mode or not courserun_has_verified_mode
    union
    select
        courserun_id
        , cast(coalesce(page_min_price, 0) as decimal(10, 2)) as price
    from runs
    where is_priced and courserun_has_verified_mode
    union
    select
        courserun_id
        , cast(coalesce(page_max_price, 0) as decimal(10, 2)) as price
    from runs
    where is_priced and courserun_has_verified_mode
)

, prices as (
    select
        courserun_id
        , array_agg(price order by price) as prices
    from price_rows
    group by courserun_id
)

select
    runs.course_readable_id as readable_id
    , runs.courserun_readable_id as run_id
    , runs.courserun_title as title
    , coalesce(runs.courserun_start_on, runs.courserun_enrollment_start_on) as start_date
    , runs.courserun_end_on as end_date
    , runs.courserun_enrollment_start_on as enrollment_start
    , runs.courserun_enrollment_end_on as enrollment_end
    , (not runs.courserun_is_variant and runs.courserun_is_enrollable and runs.page_is_live) as published
    , runs.courserun_is_b2b_only as b2b_only
    , runs.courserun_is_variant as is_variant
    , if(runs.has_page and runs.courserun_is_fully_enrollable, 'Current', 'Archived') as status
    , if(coalesce(runs.courserun_is_self_paced, false), 'self_paced', 'instructor_paced') as pace
    , runs.courserun_language as language
    , runs.courserun_course_number as course_number
    , runs.courserun_has_verified_mode as has_verified_mode
    , prices.prices
    , runs.page_instructors as instructors
from runs
inner join prices on runs.courserun_id = prices.courserun_id
