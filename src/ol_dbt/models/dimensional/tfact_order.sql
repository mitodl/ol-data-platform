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
        , order_created_on as order_updated_on  -- mitxonline doesn't have updated_on, use created_on
        , '{{ var("mitxonline") }}' as platform
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
        , '{{ var("mitxpro") }}' as platform
    from {{ ref('int__mitxpro__ecommerce_order') }}
)

, combined_orders as (
    select * from mitxonline_orders
    union all
    select * from mitxpro_orders
)

-- Join to dimensions for FKs
, orders_with_fks as (
    select
        combined_orders.*
        , cast(null as integer) as user_fk  -- dim_user not in Phase 1-2
        , cast(null as integer) as platform_fk  -- dim_platform not in Phase 1-2
        , case when order_created_on is not null
            then cast(date_format(date_parse(substr(order_created_on, 1, 19), '%Y-%m-%dT%H:%i:%s'), '%Y%m%d') as integer)
            else null end as order_date_key
        , case when order_updated_on is not null
            then cast(date_format(date_parse(substr(order_updated_on, 1, 19), '%Y-%m-%dT%H:%i:%s'), '%Y%m%d') as integer)
            else null end as order_updated_date_key
    from combined_orders
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
        , platform
        , order_state
        , order_total_price_paid
        , order_reference_number
        , order_updated_on
    from orders_with_fks

    {% if is_incremental() %}
    where order_updated_on > (select max(order_updated_on) from {{ this }})
    {% endif %}
)

select * from final
