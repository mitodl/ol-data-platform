{{ config(
    materialized='incremental',
    unique_key='order_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- Consolidate orders from all platforms
with mitxonline_orders as (
    select
        order_id
        , user_id
        , order_state
        , order_total_price_paid
        , order_reference_number
        , order_created_on
        , order_created_on as order_updated_on  -- mitxonline doesn't have updated_on, use created_on
        , 'mitxonline' as platform
        , product_id
        , case
            when discount_amount_text like 'Fixed Price: 0%' then 'free'
            when right(discount_amount_text, 1) = '%' then 'percentage'
            when left(discount_amount_text, 1) = '$' then 'fixed_amount'
            else null
          end as discount_type_code
    from {{ ref('int__mitxonline__ecommerce_order') }}
)

, mitxpro_orders as (
    select
        order_id
        , order_purchaser_user_id as user_id  -- mitxpro calls it order_purchaser_user_id
        , order_state
        , order_total_price_paid
        , cast(null as varchar) as order_reference_number  -- mitxpro doesn't have reference_number
        , order_created_on
        , order_updated_on
        , 'mitxpro' as platform
        , cast(null as bigint) as product_id
        , case
            when couponpaymentversion_discount_type = 'percent-off' then 'percentage'
            when couponpaymentversion_discount_type = 'dollars-off' then 'fixed_amount'
            else null
          end as discount_type_code
    from {{ ref('int__mitxpro__ecommerce_order') }}
)

, combined_orders as (
    select * from mitxonline_orders
    union all
    select * from mitxpro_orders
)

-- Join to dimensions for FKs
, dim_discount_type as (
    select discount_type_pk, discount_type_code
    from {{ ref('dim_discount_type') }}
)

, dim_product as (
    select product_pk, source_product_id, platform
    from {{ ref('dim_product') }}
    where is_current = true
)

, orders_with_fks as (
    select
        combined_orders.*
        , cast(null as varchar) as user_fk  -- dim_user: user_pk is varchar (surrogate key)
        , cast(null as varchar) as platform_fk  -- dim_platform not in Phase 1-2
        , dim_discount_type.discount_type_pk as discount_type_fk
        , dim_product.product_pk as product_fk
        , {{ iso8601_to_date_key('order_created_on') }} as order_date_key
        , {{ iso8601_to_date_key('order_updated_on') }} as order_updated_date_key
    from combined_orders
    left join dim_discount_type on combined_orders.discount_type_code = dim_discount_type.discount_type_code
    left join dim_product
        on cast(combined_orders.product_id as varchar) = cast(dim_product.source_product_id as varchar)
        and combined_orders.platform = dim_product.platform
)

, final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'cast(order_id as varchar)',
            'platform'
        ]) }} as order_key
        , order_id
        , order_date_key
        , order_updated_date_key
        , user_fk
        , platform_fk
        , discount_type_fk
        , product_fk
        , platform
        , order_state
        , order_total_price_paid
        , order_reference_number
        , order_updated_on
    from orders_with_fks

    {% if is_incremental() %}
    where order_updated_on >= (select max(order_updated_on) from {{ this }})
    {% endif %}
)

select * from final
