{{ config(
    materialized='table'
) }}

with mitxonline_discounts as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__ecommerce_discount') }}
)
, mitxpro_coupons as (
    select *
    from {{ ref('int__mitxpro__ecommerce_allcoupons') }}
)

,micromasters_discounts as (
    select *
    from {{ ref('stg__micromasters__app__postgres__ecommerce_coupon') }}
)

, combined_discounts as (
    select
        'mitxonline' as platform_code
        , discount_id as source_discount_id
        , discount_code
        , discount_type
        , discount_amount
        , discount_redemption_type as redemption_type
        , discount_source
        , discount_max_redemptions as max_redemptions
        , discount_activated_on as activated_on
        , discount_expires_on as expires_on
        , discount_created_on as created_on
        , discount_updated_on as updated_on
    from mitxonline_discounts

    union all

    select
        'mitxpro' as platform_code
        , coupon_id as source_discount_id
        , coupon_code as discount_code
        , discount_type
        , case
             when discount_type = 'percent-off'
                then discount_amount_numeric * 100
             else discount_amount_numeric
         end as discount_amount
        , coupon_type as redemption_type
        , case when coupon_id is null then 'b2b' else discount_source end as discount_source
        , max_redemptions
        , activated_on
        , expires_on
        , coupon_created_on as created_on
        , coupon_updated_on as updated_on
    from mitxpro_coupons

    union all

    select
        'micromasters' as platform_code
        , coupon_id as source_discount_id
        , coupon_code as discount_code
        , coupon_amount_type as discount_type
        , coupon_amount as discount_amount
        , null as redemption_type
        , null as discount_source
        , 1 as max_redemptions
        , coupon_activated_on as activated_on
        , coupon_expires_on as expires_on
        , coupon_created_on as created_on
        , coupon_updated_on as updated_on
    from micromasters_discounts
)

-- A discount can be re-saved under a new discount_code over its lifetime, so
-- dedupe to one row per (source_discount_id, platform_code), keeping the most
-- recently updated code, to match the documented grain and prevent downstream
-- fact joins keyed on (source_discount_id, platform_code) from fanning out.
, deduped_discounts as (
    select
        *
        , row_number() over (
            partition by platform_code, source_discount_id
            -- discount_code as a final tie-breaker keeps the pick deterministic when
            -- updated_on/created_on are identical (or both null) across duplicate rows
            order by updated_on desc nulls last, created_on desc nulls last, discount_code desc nulls last
        ) as _row_num
    from combined_discounts
)

select
    {{ dbt_utils.generate_surrogate_key(['cast(source_discount_id as varchar)', 'platform_code']) }} as discount_pk
    , platform_code
    , source_discount_id
    , discount_code
    , discount_type
    , discount_amount
    , discount_source
    , max_redemptions
    , redemption_type
    , activated_on
    , expires_on
    , created_on
    , updated_on
from deduped_discounts
where _row_num = 1
