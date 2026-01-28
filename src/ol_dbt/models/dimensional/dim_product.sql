{{ config(
    materialized='incremental',
    unique_key='product_pk'
) }}

with mitxonline_products as (
    select
        product_id
        , product_type
        , courserun_id
        , program_id
        , product_price
        , product_is_active
        , product_created_on
        , '{{ var("mitxonline") }}' as platform
    from {{ ref('int__mitxonline__ecommerce_product') }}
)

, mitxpro_products as (
    select
        product_id
        , product_type
        , courserun_id
        , program_id
        , product_list_price as product_price
        , product_is_active
        , product_created_on
        , '{{ var("mitxpro") }}' as platform
    from {{ ref('int__mitxpro__ecommerce_product') }}
)

, combined_products as (
    select * from mitxonline_products
    union all
    select * from mitxpro_products
)

-- Join to get courserun_fk and program_fk
, dim_course_run as (
    select courserun_pk, source_id, platform_fk
    from {{ ref('dim_course_run') }}
    where is_current = true
)

, dim_program as (
    select program_pk, source_id, platform_fk
    from {{ ref('dim_program') }}
)

-- dim_platform not in Phase 1-2, setting platform_fk to null
--, dim_platform as (
--    select platform_pk, platform_readable_id
--    from {{ ref('dim_platform') }}
--)

, products_with_fks as (
    select
        combined_products.*
        , dim_course_run.courserun_pk as courserun_fk
        , dim_program.program_pk as program_fk
        , cast(null as integer) as platform_fk  -- dim_platform not in Phase 1-2
        , case when product_created_on is not null
            then cast(date_format(
                case when length(product_created_on) = 10
                    then date_parse(product_created_on, '%Y-%m-%d')
                    else date_parse(substr(product_created_on, 1, 19), '%Y-%m-%dT%H:%i:%s')
                end, '%Y%m%d') as integer)
            else null end as created_date_key
    from combined_products
    left join dim_course_run
        on combined_products.courserun_id = dim_course_run.source_id
    left join dim_program
        on combined_products.program_id = dim_program.source_id
)

, final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'cast(product_id as varchar)',
            'platform',
            'cast(current_timestamp as varchar)'
        ]) }} as product_pk
        , product_id as source_product_id
        , product_type
        , courserun_fk
        , program_fk
        , platform_fk
        , product_price
        , 'USD' as product_currency
        , product_is_active
        , created_date_key
        , current_timestamp as effective_date
        , cast(null as timestamp) as end_date
        , true as is_current
    from products_with_fks

    {% if is_incremental() %}
    -- Track price changes with SCD Type 2
    where not exists (
        select 1
        from {{ this }} as existing
        where
            existing.source_product_id = products_with_fks.product_id
            and existing.platform_fk = products_with_fks.platform_fk
            and existing.is_current = true
            and existing.product_price = products_with_fks.product_price
    )
    {% endif %}
)

select * from final
