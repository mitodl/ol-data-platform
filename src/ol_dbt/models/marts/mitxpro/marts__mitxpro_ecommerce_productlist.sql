with ecommerce_product as (
    select *
    from {{ ref('int__mitxpro__ecommerce_product') }}
)

, ecommerce_productversion as (
    select *
    from {{ ref('int__mitxpro__ecommerce_productversion') }}
)

, course_runs as (
    select *
    from {{ ref('int__mitxpro__course_runs') }}
)

, programs as (
    select *
    from {{ ref('int__mitxpro__programs') }}
)

, program_runs as (
    select *
    from {{ ref('int__mitxpro__program_runs') }}
)

, coursesinprogram as (
    select *
    from {{ ref('int__mitxpro__coursesinprogram') }}
)

, courses as (
    select *
    from {{ ref('int__mitxpro__courses') }}
)

, course_to_topics as (
    select *
    from {{ ref('int__mitxpro__courses_to_topics') }}
)

, coursetopic as (
    select *
    from {{ ref('int__mitxpro__coursetopic') }}
)

, ecommerce_productversion_latest as (
    select *
    from (
        select
            *
            , rank() over (partition by product_id order by productversion_updated_on desc) as myrank
        from ecommerce_productversion
    ) as a
    where myrank = 1
    order by product_id
)

select
    'xPRO' as product_platform
    , ecommerce_product.product_id as eproductid
    , course_runs.courserun_title as product_name
    , course_runs.courserun_readable_id as product_id
    , ecommerce_product.product_type
    , ecommerce_productversion_latest.productversion_price as list_price
    , ecommerce_productversion_latest.productversion_description as product_description
    , course_runs.courserun_start_on as start_date
    , course_runs.courserun_end_on as end_date
    , course_runs.courserun_enrollment_start_on as enrollment_start
    , course_runs.courserun_enrollment_end_on as enrollment_end
    , concat(
        '<a href="https://xpro.mit.edu/checkout?product='
        , cast(ecommerce_product.product_id as varchar (50))
        , '">', course_runs.courserun_readable_id, '</a>'
    ) as link
    , concat(programs.program_readable_id, '+', course_runs.courserun_tag) as product_parent_run_id
    , courses.cms_coursepage_duration as duration
    , courses.cms_coursepage_time_commitment as time_commitment
    , coursetopic.coursetopic_name
from ecommerce_product
left join ecommerce_productversion_latest
    on ecommerce_product.product_id = ecommerce_productversion_latest.product_id
inner join course_runs
    on ecommerce_product.courserun_id = course_runs.courserun_id
left join coursesinprogram
    on course_runs.course_id = coursesinprogram.course_id
left join programs
    on coursesinprogram.program_id = programs.program_id
left join courses
    on course_runs.course_id = courses.course_id
left join course_to_topics
    on course_runs.course_id = course_to_topics.course_id
left join coursetopic
    on course_to_topics.coursetopic_id = coursetopic.coursetopic_id
where ecommerce_product.product_type = 'course run'

union all

select
    'xPRO' as product_platform
    , ecommerce_product.product_id as eproductid
    , program_runs.program_title as product_name
    , program_runs.programrun_readable_id as product_id
    , 'programrun' as product_type
    , ecommerce_productversion_latest.productversion_price as list_price
    , ecommerce_productversion_latest.productversion_description as product_description
    , program_runs.programrun_start_on as start_date
    , program_runs.programrun_end_on as end_date
    , null as enrollment_start
    , null as enrollment_end
    , concat(
        '<a href="https://xpro.mit.edu/checkout?product='
        , program_runs.programrun_readable_id
        , '">', program_runs.programrun_readable_id, '</a>'
    ) as link
    , null as product_parent_run_id
    , programs.cms_programpage_duration as duration
    , programs.cms_programpage_time_commitment as time_commitment
    , null as coursetopic_name
from ecommerce_product
left join ecommerce_productversion_latest
    on ecommerce_product.product_id = ecommerce_productversion_latest.product_id
inner join programs
    on ecommerce_product.program_id = programs.program_id
inner join program_runs
    on programs.program_id = program_runs.program_id
where ecommerce_product.product_type = 'program'
