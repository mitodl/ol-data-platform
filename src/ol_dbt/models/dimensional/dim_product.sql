{{ config(
    materialized='incremental',
    unique_key='product_pk',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
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
        , 'mitxonline' as platform
        , 'mitxonline' as platform_code
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
        , 'mitxpro' as platform
        , 'mitxpro' as platform_code
    from {{ ref('int__mitxpro__ecommerce_product') }}
)

, combined_products as (
    select * from mitxonline_products
    union all
    select * from mitxpro_products
)

-- Join to get courserun_fk and program_fk
, dim_course_run as (
    select courserun_pk, source_id, platform_fk, platform
    from {{ ref('dim_course_run') }}
    where is_current = true
)

, dim_program as (
    select program_pk, source_id, platform_fk, platform_code
    from {{ ref('dim_program') }}
)

-- dim_platform not in Phase 1-2, setting platform_fk to null

, dim_platform_lookup as (
    select platform_pk, platform_readable_id
    from {{ ref('dim_platform') }}
)

, products_with_fks as (
    select
        combined_products.*
        , dim_course_run.courserun_pk as courserun_fk
        , dim_program.program_pk as program_fk
        , dim_platform_lookup.platform_pk as platform_fk
        , {{ iso8601_to_date_key('product_created_on') }} as created_date_key
    from combined_products
    left join dim_course_run
        on combined_products.courserun_id = dim_course_run.source_id
        and combined_products.platform = dim_course_run.platform
    left join dim_program
        on combined_products.program_id = dim_program.source_id
        and combined_products.platform_code = dim_program.platform_code
    left join dim_platform_lookup
        on combined_products.platform = dim_platform_lookup.platform_readable_id
)

, final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'cast(product_id as varchar)',
            'platform'
        ]) }} as product_pk
        , product_id as source_product_id
        , product_type
        , courserun_fk
        , program_fk
        , platform_fk
        , platform
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
            and existing.platform = products_with_fks.platform
            and existing.is_current = true
            and existing.product_price = products_with_fks.product_price
    )
    {% endif %}
)

{% if is_incremental() %}
-- Expire prior current rows when price changes
, records_to_expire as (
    select
        existing.product_pk
        , existing.source_product_id
        , existing.product_type
        , existing.courserun_fk
        , existing.program_fk
        , existing.platform_fk
        , existing.platform
        , existing.product_price
        , existing.product_currency
        , existing.product_is_active
        , existing.created_date_key
        , existing.effective_date
        , current_timestamp as end_date
        , false as is_current
    from {{ this }} as existing
    inner join final as new_records
        on existing.source_product_id = new_records.source_product_id
        and existing.platform = new_records.platform
    where existing.is_current = true
)

, combined as (
    select * from final
    union all
    select * from records_to_expire
)

select * from combined
{% else %}
select * from final
{% endif %}
