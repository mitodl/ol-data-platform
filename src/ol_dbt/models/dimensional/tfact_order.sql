{{ config(
    materialized='incremental',
    unique_key='order_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- E-commerce order line fact table. Grain: one row per (order_id, line_id, platform).
-- All source models are natively at line grain (one product per line).
-- order_total_price_paid is a repeated order-header value — NOT additive across lines
-- of the same order. Use line_price for revenue aggregations.
with mitxonline_orders as (
    -- int__mitxonline__ecommerce_order grain: (order_id, line_id)
    select
        cast(line_id as varchar) as line_id
        , order_id
        , user_id
        , product_id
        , product_price as line_price
        , order_state
        , order_total_price_paid
          --- placeholder for tax information on MITx Online orders, which is not currently implemented.
        , order_total_price_paid as order_total_price_paid_plus_tax
        , null as order_tax_amount
        , null as order_tax_rate
        , null as order_tax_country_code
        , order_reference_number
        , order_created_on
        , order_updated_on
        , 'mitxonline' as platform
        , discount_id
        , discount_code
        , case
            when discount_amount_text like 'Fixed Price: 0%' then 'free'
            when substr(discount_amount_text, length(discount_amount_text), 1) = '%' then 'percentage'
            when substr(discount_amount_text, 1, 1) = '$' then 'fixed_amount'
            else null
          end as discount_type_code
    from {{ ref('int__mitxonline__ecommerce_order') }}
)

, mitxpro_orders as (
    -- Join order header to lines to achieve (order_id, line_id) grain.
    -- int__mitxpro__ecommerce_order is at order grain; int__mitxpro__ecommerce_line has lines.
    select
        cast(lines.line_id as varchar) as line_id
        , orders.order_id
        , orders.order_purchaser_user_id as user_id
        , lines.product_id
        , lines.product_price as line_price
        , orders.order_state
        , orders.order_total_price_paid
        , orders.order_total_price_paid_plus_tax
        , orders.order_tax_amount
        , orders.order_tax_rate
        , orders.order_tax_country_code
        , cast(null as varchar) as order_reference_number
        , orders.order_created_on
        , orders.order_updated_on
        , 'mitxpro' as platform
        , coupon_id as discount_id
        , coupon_code as discount_code
        , case
            when orders.couponpaymentversion_discount_type = 'percent-off' then 'percentage'
            when orders.couponpaymentversion_discount_type = 'dollars-off' then 'fixed_amount'
            else null
          end as discount_type_code
    from {{ ref('int__mitxpro__ecommerce_order') }} as orders
    inner join {{ ref('int__mitxpro__ecommerce_line') }} as lines
        on orders.order_id = lines.order_id
)

, micromasters_orders as (
    -- int__micromasters__orders grain: (order_id, line_id)
    select
        cast(line_id as varchar) as line_id
        , order_id
        , user_id
        , cast(null as bigint) as product_id  -- no dim_product coverage for micromasters
        , line_price
        , order_state
        , order_total_price_paid
        , order_total_price_paid as order_total_price_paid_plus_tax
        , null as order_tax_amount
        , null as order_tax_rate
        , null as order_tax_country_code
        , cast(null as varchar) as order_reference_number
        , order_created_on
        , order_created_on as order_updated_on  -- micromasters has no updated_on
        , 'micromasters' as platform
        , coupon_id as discount_id
        , coupon_code as discount_code
        , null as discount_type_code
    from {{ ref('int__micromasters__orders') }}
)

, combined_orders as (
    select * from mitxonline_orders
    union all
    select * from mitxpro_orders
    union all
    select * from micromasters_orders
)

, user_lookup as (
    select
        user_pk
        , mitxonline_application_user_id
        , mitxpro_application_user_id
        , micromasters_user_id
    from {{ ref('dim_user') }}
    where user_pk is not null
)

, dim_discount_type as (
    select discount_type_pk, discount_type_code
    from {{ ref('dim_discount_type') }}
)

, dim_product as (
    select product_pk, source_product_id, platform
    from {{ ref('dim_product') }}
    where is_current = true
)

, dim_platform_lookup as (
    select platform_pk, platform_readable_id
    from {{ ref('dim_platform') }}
)

, dim_discount as (
    select discount_pk, source_discount_id, platform_code
    from {{ ref('dim_discount') }}
)

, dim_tax_rate as (
    select tax_rate_pk, country_code
    from {{ ref('dim_tax_rate') }}
)

, orders_with_fks as (
    select
        combined_orders.*
        , coalesce(
            case when combined_orders.platform = 'mitxonline'
                then ul_mitxonline.user_pk
            end,
            case when combined_orders.platform = 'mitxpro'
                then ul_mitxpro.user_pk
            end,
            case when combined_orders.platform = 'micromasters'
                then ul_micromasters.user_pk
            end
        ) as user_fk
        , dim_platform_lookup.platform_pk as platform_fk
        , dim_discount_type.discount_type_pk as discount_type_fk
        , dim_product.product_pk as product_fk
        , dim_discount.discount_pk as discount_fk
        , dim_tax_rate.tax_rate_pk as tax_rate_fk
        , {{ iso8601_to_date_key('order_created_on') }} as order_date_key
        , {{ iso8601_to_date_key('order_updated_on') }} as order_updated_date_key
    from combined_orders
    left join user_lookup as ul_mitxonline
        on combined_orders.platform = 'mitxonline'
        and combined_orders.user_id = ul_mitxonline.mitxonline_application_user_id
    left join user_lookup as ul_mitxpro
        on combined_orders.platform = 'mitxpro'
        and combined_orders.user_id = ul_mitxpro.mitxpro_application_user_id
    left join user_lookup as ul_micromasters
        on combined_orders.platform = 'micromasters'
        and combined_orders.user_id = ul_micromasters.micromasters_user_id
    left join dim_platform_lookup
        on combined_orders.platform = dim_platform_lookup.platform_readable_id
    left join dim_discount_type
        on combined_orders.discount_type_code = dim_discount_type.discount_type_code
    left join dim_product
        on cast(combined_orders.product_id as varchar) = cast(dim_product.source_product_id as varchar)
        and combined_orders.platform = dim_product.platform
    left join dim_discount
        on combined_orders.discount_id = dim_discount.source_discount_id
        and combined_orders.platform = dim_discount.platform_code
    left join dim_tax_rate
        on combined_orders.order_tax_country_code = dim_tax_rate.country_code
)

{% if is_incremental() %}
-- Pre-compute per-platform watermarks to avoid the correlated subquery anti-pattern.
-- Without this, Trino fans out to (left rows × target rows per platform) intermediate
-- rows for the AssignUniqueId + LeftJoin + StreamingAggregate execution pattern.
, incremental_watermarks as (
    select platform as watermark_platform, max(order_updated_on) as max_updated_on
    from {{ this }}
    group by platform
)
{% endif %}

, final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'cast(order_id as varchar)',
            'line_id',
            'platform'
        ]) }} as order_key
        , order_id
        , line_id
        , order_date_key
        , order_updated_date_key
        , user_fk
        , platform_fk
        , discount_fk
        , discount_type_fk
        , product_fk
        , owf.platform
        , order_state
        , line_price
        , order_total_price_paid
        , tax_rate_fk
        , order_total_price_paid_plus_tax
        , order_tax_amount
        , order_tax_rate
        , order_reference_number
        , order_updated_on
    from orders_with_fks as owf

    {% if is_incremental() %}
    -- Per-platform max prevents xPro updates from advancing the global watermark
    -- past MITx Online order creation times, which would cause silent data loss.
    -- left join preserves orders from platforms not yet in the target table
    left join incremental_watermarks w on w.watermark_platform = owf.platform
    where (
        w.max_updated_on is null  -- platform not yet in target, include all
        or owf.order_updated_on >= w.max_updated_on
        or owf.order_updated_on is null
    )
    {% endif %}
)

select * from final
