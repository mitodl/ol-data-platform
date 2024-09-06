with mitxonline_product as (
    select * from {{ ref('int__mitxonline__ecommerce_product') }}
)

, mitxonline_course_runs as (
    select * from {{ ref('int__mitxonline__course_runs') }}
)

, mitxpro_product as (
    select * from {{ ref('int__mitxpro__ecommerce_product') }}
)

, mitxpro_course_runs as (
    select * from {{ ref('int__mitxpro__course_runs') }}
)

, mitxpro_courses as (
    select * from {{ ref('int__mitxpro__courses') }}
)

, mitxpro_programs as (
    select * from {{ ref('int__mitxpro__programs') }}
)

, mitxpro_program_runs as (
    select * from {{ ref('int__mitxpro__program_runs') }}
)

, mitxonline_product_view as (
    select
        mitxonline_product.product_id
        , mitxonline_product.product_type
        , mitxonline_product.courserun_readable_id as product_readable_id
        , mitxonline_product.product_price as list_price
        , mitxonline_product.product_description
        , mitxonline_product.product_is_active
        , mitxonline_product.product_created_on
        , mitxonline_course_runs.courserun_title as product_name
        , mitxonline_course_runs.courserun_start_on as start_on
        , mitxonline_course_runs.courserun_end_on as end_on
        , mitxonline_course_runs.courserun_enrollment_start_on as enrollment_start_on
        , mitxonline_course_runs.courserun_enrollment_end_on as enrollment_end_on
        , mitxonline_course_runs.courserun_upgrade_deadline
    from mitxonline_product
    left join mitxonline_course_runs
        on mitxonline_product.courserun_id = mitxonline_course_runs.courserun_id
)

, mitxpro_product_view as (
    select
        mitxpro_product.product_id
        , mitxpro_product.product_list_price as list_price
        , mitxpro_product.product_description
        , mitxpro_product.product_is_private
        , mitxpro_product.product_is_active
        , mitxpro_product.product_created_on
        , mitxpro_course_runs.courserun_enrollment_start_on as enrollment_start_on
        , mitxpro_course_runs.courserun_enrollment_end_on as enrollment_end_on
        , if(mitxpro_product.product_type = 'program', 'program run', mitxpro_product.product_type) as product_type
        , coalesce(mitxpro_courses.platform_name, mitxpro_programs.platform_name) as product_platform
        , coalesce(
            mitxpro_course_runs.courserun_readable_id, mitxpro_program_runs.programrun_readable_id
        ) as product_readable_id
        , coalesce(mitxpro_course_runs.courserun_title, mitxpro_program_runs.program_title) as product_name
        , coalesce(mitxpro_course_runs.courserun_start_on, mitxpro_program_runs.programrun_start_on) as start_on
        , coalesce(mitxpro_course_runs.courserun_end_on, mitxpro_program_runs.programrun_end_on) as end_on
    from mitxpro_product
    left join mitxpro_course_runs
        on mitxpro_product.courserun_id = mitxpro_course_runs.courserun_id
    left join mitxpro_courses
        on mitxpro_course_runs.course_id = mitxpro_courses.course_id
    left join mitxpro_program_runs
        on mitxpro_product.program_id = mitxpro_program_runs.program_id
    left join mitxpro_programs
        on mitxpro_program_runs.program_id = mitxpro_programs.program_id
    where mitxpro_product.product_type in ('program', 'course run')
)

select
    '{{ var("mitxonline") }}' as platform
    , '{{ var("mitxonline") }}' as product_platform
    , product_readable_id
    , product_name
    , product_id
    , product_type
    , product_description
    , list_price
    , product_is_active
    , false as product_is_private
    , product_created_on
    , start_on
    , end_on
    , enrollment_start_on
    , enrollment_end_on
    , courserun_upgrade_deadline
from mitxonline_product_view

union all

select
    '{{ var("mitxpro") }}' as platform
    , product_platform
    , product_readable_id
    , product_name
    , product_id
    , product_type
    , product_description
    , list_price
    , product_is_active
    , product_is_private
    , product_created_on
    , start_on
    , end_on
    , enrollment_start_on
    , enrollment_end_on
    , null as courserun_upgrade_deadline
from mitxpro_product_view
