{{ config(
    materialized='incremental',
    unique_key='order_key',
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
        , order_updated_on
        , '{{ var("mitxonline") }}' as platform
    from {{ ref('int__mitxonline__ecommerce_order') }}
)

, mitxpro_orders as (
    select
        order_id
        , user_id
        , order_state
        , order_total_price_paid
        , order_reference_number
        , order_created_on
        , order_updated_on
        , '{{ var("mitxpro") }}' as platform
    from {{ ref('int__mitxpro__ecommerce_order') }}
)

, combined_orders as (
    select * from mitxonline_orders
    union all
    select * from mitxpro_orders
)

-- Join to dimensions for FKs
, dim_user as (
    select user_pk, mitxonline_user_id, mitxpro_user_id
    from {{ ref('dim_user') }}
)

, dim_platform as (
    select platform_pk, platform_readable_id
    from {{ ref('dim_platform') }}
)

, orders_with_fks as (
    select
        combined_orders.*
        , dim_user.user_pk as user_fk
        , dim_platform.platform_pk as platform_fk
        , cast(format_datetime(order_created_on, 'yyyyMMdd') as integer) as order_date_key
        , cast(format_datetime(order_updated_on, 'yyyyMMdd') as integer) as order_updated_date_key
    from combined_orders
    left join dim_platform on combined_orders.platform = dim_platform.platform_readable_id
    left join dim_user
        on
            (combined_orders.platform = '{{ var("mitxonline") }}' and combined_orders.user_id = dim_user.mitxonline_user_id)
            or (combined_orders.platform = '{{ var("mitxpro") }}' and combined_orders.user_id = dim_user.mitxpro_user_id)
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
        , order_state
        , order_total_price_paid
        , order_reference_number
    from orders_with_fks

    {% if is_incremental() %}
    where order_updated_on > (select max(order_updated_on) from {{ this }})
    {% endif %}
)

select * from final
