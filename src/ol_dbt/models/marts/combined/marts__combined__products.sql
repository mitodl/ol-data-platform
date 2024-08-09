with mitxonline_product as (
    select * from {{ ref('int__mitxonline__ecommerce_product') }}
)

, mitxonline_runs as (
    select * from {{ ref('int__mitxonline__course_runs') }}
)

, mitxpro_product as (
    select * from {{ ref('int__mitxpro__ecommerce_product') }}
)

, mitxpro_runs as (
    select * from {{ ref('int__mitxpro__course_runs') }}
)


select
    '{{ var("mitxonline") }}' as platform
    , mitxonline_product.product_id
    , mitxonline_product.product_type
    , mitxonline_product.product_readable_id
    , mitxonline_product.product_price
    , mitxonline_runs.courserun_start_on
    , mitxonline_runs.courserun_end_on
    , mitxonline_runs.courserun_enrollment_start_on
    , mitxonline_runs.courserun_enrollment_end_on
from mitxonline_product
left join mitxonline_runs
    on mitxonline_product.courserun_id = mitxonline_runs.courserun_id

union all

select
    '{{ var("mitxpro") }}' as platform
    , mitxpro_product.product_id
    , mitxpro_product.pproduct_type
    , mitxpro_product.pproduct_readable_id
    , null as pproduct_price
    , mitxpro_runs.courserun_start_on
    , mitxpro_runs.courserun_end_on
    , mitxpro_runs.courserun_enrollment_start_on
    , mitxpro_runs.courserun_enrollment_end_on
from mitxpro_product
left join mitxpro_runs
    on mitxpro_product.courserun_id = mitxpro_runs.courserun_id
