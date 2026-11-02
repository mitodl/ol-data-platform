{{ config(
    materialized='incremental',
    unique_key='order_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    post_hook="delete from {{ this }}
    where platform = 'micromasters'
    and order_id in (
        select order_id
        from {{ ref('int__micromasters__orders') }}
        where order_is_migrated_to_mitxonline
    )"
) }}

{#- The first incremental run after discount_amount is added reads a target without the column.
    In a unit test `this` is the fixture's name, a string, which always has it. -#}
{%- set target_has_discount_amount = false -%}
{%- if is_incremental() and this is string -%}
    {%- set target_has_discount_amount = true -%}
{%- elif is_incremental() -%}
    {%- set target_columns = adapter.get_columns_in_relation(this) | map(attribute='name') | map('lower') | list -%}
    {%- set target_has_discount_amount = 'discount_amount' in target_columns -%}
{%- endif %}

-- The post_hook deletes MicroMasters orders counted under mitxonline, which delete+insert
-- would otherwise keep: it only deletes keys the run selects.
-- E-commerce order line fact table. Grain: one row per (order_id, line_id, platform).
-- All source models are natively at line grain (one product per line).
-- order_total_price_paid is a repeated order-header value — NOT additive across lines
-- of the same order. line_price is the list price at purchase; subtract discount_amount
-- for what the line was charged.
with mitxonline_orders as (
    -- int__mitxonline__ecommerce_order grain: (order_id, line_id)
    select
        cast(line_id as varchar) as line_id
        , order_id
        , user_id
        , product_id
        , line_price
        , discount_amount
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
        , line_discounted_unit_price as line_price_paid
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
        -- Every multi-line xPRO order has a total of 0, so the header total is each line's
        -- price paid (assert_xpro_multi_line_orders_paid_nothing warns if that changes)
        , lines.product_price - orders.order_total_price_paid as discount_amount
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
        , orders.coupon_id as discount_id
        , orders.coupon_code as discount_code
        , orders.order_total_price_paid as line_price_paid
    from {{ ref('int__mitxpro__ecommerce_order') }} as orders
    inner join {{ ref('int__mitxpro__ecommerce_line') }} as lines
        on orders.order_id = lines.order_id
)

, bootcamps_orders as (
    -- int__bootcamps__ecommerce_order grain: (order_id, line_id)
    -- Bootcamps uses personal pricing; discounts are not modeled — revenue is in line_price / order_total_price_paid only.
    select
        cast(line_id as varchar) as line_id
        , order_id
        , order_purchaser_user_id as user_id
        , cast(null as bigint) as product_id  -- no dim_product coverage for bootcamps
        , line_price
        , cast(null as decimal(38, 2)) as discount_amount
        , order_state
        , order_total_price_paid
        , order_total_price_paid as order_total_price_paid_plus_tax
        , cast(null as decimal(38, 2)) as order_tax_amount
        , cast(null as decimal(38, 4)) as order_tax_rate
        , cast(null as varchar) as order_tax_country_code
        , order_reference_number
        , order_created_on
        , order_updated_on
        , 'bootcamps' as platform
        , cast(null as bigint) as discount_id
        , cast(null as varchar) as discount_code
        , order_total_price_paid as line_price_paid
    from {{ ref('int__bootcamps__ecommerce_order') }}
)

, micromasters_orders as (
    -- int__micromasters__orders grain: (order_id, line_id)
    select
        cast(line_id as varchar) as line_id
        , order_id
        , user_id
        , cast(null as bigint) as product_id  -- no dim_product coverage for micromasters
        , line_price
        , cast(null as decimal(38, 2)) as discount_amount  -- line_price is already post-coupon
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
        , order_total_price_paid as line_price_paid
    from {{ ref('int__micromasters__orders') }}
    -- counted under mitxonline
    where not order_is_migrated_to_mitxonline
)

, combined_orders as (
    select * from mitxonline_orders
    union all
    select * from mitxpro_orders
    union all
    select * from bootcamps_orders
    union all
    select * from micromasters_orders
)

, user_lookup as (
    select
        user_pk
        , mitxonline_application_user_id
        , mitxpro_application_user_id
        , micromasters_user_id
        , bootcamps_application_user_id
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
    select discount_pk, source_discount_id, platform_code, discount_type_code
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
            case when combined_orders.platform = 'bootcamps'
                then ul_bootcamps.user_pk
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
    left join user_lookup as ul_bootcamps
        on combined_orders.platform = 'bootcamps'
        and combined_orders.user_id = ul_bootcamps.bootcamps_application_user_id
    left join user_lookup as ul_micromasters
        on combined_orders.platform = 'micromasters'
        and combined_orders.user_id = ul_micromasters.micromasters_user_id
    left join dim_platform_lookup
        on combined_orders.platform = dim_platform_lookup.platform_readable_id
    left join dim_discount
        on combined_orders.discount_id = dim_discount.source_discount_id
        and combined_orders.platform = dim_discount.platform_code
    -- free describes the line, not the discount, so a 100% discount and a fixed price
    -- of 0 count the same way
    left join dim_discount_type
        on case
            when combined_orders.line_price_paid = 0 then 'free'
            else dim_discount.discount_type_code
        end = dim_discount_type.discount_type_code
    left join dim_product
        on cast(combined_orders.product_id as varchar) = cast(dim_product.source_product_id as varchar)
        and combined_orders.platform = dim_product.platform
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

-- Snapshot of the target's current (order_key, user_fk) pairs, used to re-select rows
-- whose user_fk has gone stale after a dim_user re-key (the source row itself did not
-- change, so the activity-timestamp watermark alone would never catch it). One row per
-- key: a key a past run wrote twice would otherwise match each source row twice and be
-- written twice again. It is re-selected instead, so delete+insert leaves one copy.
-- The stored measures do the same job for a rule change, which the watermark can't see.
, stale_user_fk_lookup as (
    select
        order_key
        , max(user_fk) as stored_user_fk
        , max(line_price) as stored_line_price
        , max(discount_type_fk) as stored_discount_type_fk
        , {% if target_has_discount_amount %}max(discount_amount){% else %}cast(null as decimal(38, 2)){% endif %}
            as stored_discount_amount
        , count(*) > 1 as stored_has_copies
    from {{ this }}
    group by order_key
)
{% endif %}

, final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'cast(order_id as varchar)',
            'line_id',
            'platform'
        ]) }} as order_key
        , owf.order_id
        , owf.line_id
        , owf.order_date_key
        , owf.order_updated_date_key
        , owf.user_fk
        , owf.platform_fk
        , owf.discount_fk
        , owf.discount_type_fk
        , owf.product_fk
        , owf.platform
        , owf.order_state
        , owf.line_price
        , owf.discount_amount
        , owf.order_total_price_paid
        , owf.tax_rate_fk
        , owf.order_total_price_paid_plus_tax
        , owf.order_tax_amount
        , owf.order_tax_rate
        , owf.order_reference_number
        , owf.order_updated_on
    from orders_with_fks as owf

    {% if is_incremental() %}
    -- Per-platform max prevents xPro updates from advancing the global watermark
    -- past MITx Online order creation times, which would cause silent data loss.
    -- left join preserves orders from platforms not yet in the target table
    left join incremental_watermarks w on w.watermark_platform = owf.platform
    left join stale_user_fk_lookup as sufk
        on sufk.order_key = {{ dbt_utils.generate_surrogate_key([
            "cast(owf.order_id as varchar)",
            "owf.line_id",
            "owf.platform"
        ]) }}
    where (
        w.max_updated_on is null  -- platform not yet in target, include all
        or owf.order_updated_on >= w.max_updated_on
        or owf.order_updated_on is null
        -- dim_user re-key: re-select rows whose resolved user_fk no longer matches the target
        or sufk.stored_user_fk is distinct from owf.user_fk
        -- a price or discount rule changed, including every row on the run that adds discount_amount
        or sufk.stored_line_price is distinct from owf.line_price
        or sufk.stored_discount_type_fk is distinct from owf.discount_type_fk
        or sufk.stored_discount_amount is distinct from owf.discount_amount
        or sufk.stored_has_copies
    )
    {% endif %}
)

-- No dedup here: each lookup above is unique on its join key, so a duplicate order_key
-- means a source or a lookup lost its grain. The error-severity unique test on order_key
-- reports it. A row_number here would keep one copy's FKs at random.
select
    order_key
    , order_id
    , line_id
    , order_date_key
    , order_updated_date_key
    , user_fk
    , platform_fk
    , discount_fk
    , discount_type_fk
    , product_fk
    , platform
    , order_state
    , line_price
    , discount_amount
    , order_total_price_paid
    , tax_rate_fk
    , order_total_price_paid_plus_tax
    , order_tax_amount
    , order_tax_rate
    , order_reference_number
    , order_updated_on
from final
