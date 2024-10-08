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
            ecommerce_productversion.*
            , rank() over (
                partition by ecommerce_productversion.product_id
                order by ecommerce_productversion.productversion_updated_on desc
            ) as myrank
        from ecommerce_productversion
    ) as a
    where myrank = 1
    order by product_id
)

, ecommerce_course_to_topics as (
    select
        course_to_topics.course_id
        , array_join(array_agg(coursetopic.coursetopic_name), ', ') as coursetopic_name
    from course_to_topics
    inner join coursetopic
        on course_to_topics.coursetopic_id = coursetopic.coursetopic_id
    group by course_to_topics.course_id
)
--there can occationally be multiple topics per course

select
    'xPRO' as product_platform
    , ecommerce_product.product_id as productid
    , course_runs.courserun_title as product_name
    , course_runs.courserun_readable_id as product_readable_id
    , ecommerce_productversion_latest.productversion_readable_id
    , ecommerce_product.product_type
    , courses.short_program_code
    , cast(ecommerce_productversion_latest.productversion_price as decimal(38, 2)) as list_price
    , ecommerce_productversion_latest.productversion_description as product_description
    , substring(course_runs.courserun_start_on, 1, 10) as start_date
    , substring(course_runs.courserun_end_on, 1, 10) as end_date
    , substring(course_runs.courserun_enrollment_start_on, 1, 10) as enrollment_start
    , substring(course_runs.courserun_enrollment_end_on, 1, 10) as enrollment_end
    , concat(
        '<a href="https://xpro.mit.edu/checkout?product='
        , cast(ecommerce_product.product_id as varchar (50))
        , '">', course_runs.courserun_readable_id, '</a>'
    ) as link
    , concat(programs.program_readable_id, '+', course_runs.courserun_tag) as product_parent_run_id
    , courses.cms_coursepage_duration as duration
    , courses.cms_coursepage_format as courseware_format
    , courses.cms_coursepage_time_commitment as time_commitment
    , ecommerce_course_to_topics.coursetopic_name as coursetopic_names
    , ecommerce_product.product_is_private
    , courses.platform_name
from ecommerce_product
left join ecommerce_productversion_latest
    on ecommerce_product.product_id = ecommerce_productversion_latest.product_id
inner join course_runs
    on ecommerce_product.courserun_id = course_runs.courserun_id
inner join courses
    on course_runs.course_id = courses.course_id
left join programs
    on courses.program_id = programs.program_id
left join ecommerce_course_to_topics
    on course_runs.course_id = ecommerce_course_to_topics.course_id
where ecommerce_product.product_type = 'course run'

union all

select
    'xPRO' as product_platform
    , ecommerce_product.product_id as productid
    , program_runs.program_title as product_name
    , program_runs.programrun_readable_id as product_readable_id
    , ecommerce_productversion_latest.productversion_readable_id
    , 'programrun' as product_type
    , program_runs.short_program_code
    , cast(ecommerce_productversion_latest.productversion_price as decimal(38, 2)) as list_price
    , ecommerce_productversion_latest.productversion_description as product_description
    , substring(program_runs.programrun_start_on, 1, 10) as start_date
    , substring(program_runs.programrun_end_on, 1, 10) as end_date
    , null as enrollment_start
    , null as enrollment_end
    , concat(
        '<a href="https://xpro.mit.edu/checkout?product='
        , program_runs.programrun_readable_id
        , '">', program_runs.programrun_readable_id, '</a>'
    ) as link
    , null as product_parent_run_id
    , programs.cms_programpage_duration as duration
    , programs.cms_programpage_format as courseware_format
    , programs.cms_programpage_time_commitment as time_commitment
    , null as coursetopic_names
    , ecommerce_product.product_is_private
    , programs.platform_name
from ecommerce_product
left join ecommerce_productversion_latest
    on ecommerce_product.product_id = ecommerce_productversion_latest.product_id
inner join programs
    on ecommerce_product.program_id = programs.program_id
inner join program_runs
    on programs.program_id = program_runs.program_id
where ecommerce_product.product_type = 'program'
