-- The course runs xPRO's catalog API (/api/courses/, /api/programs/) considers: live runs
-- with a start date. is_unexpired and current_price follow CourseRun.is_unexpired and
-- CourseRun.current_price in mitxpro's courses/models.py, evaluated when this model builds.

with runs as (
    select * from {{ ref('stg__mitxpro__app__postgres__courses_courserun') }}
)

, products as (
    select * from {{ ref('int__mitxpro__ecommerce_product') }}
)

, productversions as (
    select * from {{ ref('stg__mitxpro__app__postgres__ecommerce_productversion') }}
)

-- Product's default manager returns active products only. A run has at most one in practice;
-- the lowest id is taken if there are more.
, run_products as (
    select
        courserun_id
        , product_id
        , row_number() over (partition by courserun_id order by product_id) as product_rank
    from products
    where product_is_active and courserun_id is not null
)

-- Product.latest_version is the most recently created version
, latest_versions as (
    select
        product_id
        , productversion_price
        , row_number() over (
            partition by product_id order by productversion_created_on desc, productversion_id desc
        ) as version_rank
    from productversions
)

select
    runs.courserun_id
    , runs.course_id
    , runs.courserun_readable_id
    , runs.courserun_title
    , runs.courserun_start_on
    , runs.courserun_end_on
    , runs.courserun_enrollment_start_on
    , runs.courserun_enrollment_end_on
    , run_products.product_id
    , latest_versions.productversion_price as courserun_current_price
    , (
        (
            runs.courserun_end_on is null
            or {{ from_iso8601_timestamp('runs.courserun_end_on') }} > current_timestamp
        )
        and (
            runs.courserun_enrollment_end_on is null
            or {{ from_iso8601_timestamp('runs.courserun_enrollment_end_on') }} > current_timestamp
        )
        and (
            runs.courserun_enrollment_start_on is null
            or {{ from_iso8601_timestamp('runs.courserun_enrollment_start_on') }} <= current_timestamp
        )
    ) as courserun_is_unexpired
from runs
left join run_products
    on runs.courserun_id = run_products.courserun_id and run_products.product_rank = 1
left join latest_versions
    on run_products.product_id = latest_versions.product_id and latest_versions.version_rank = 1
where runs.courserun_is_live and runs.courserun_start_on is not null
