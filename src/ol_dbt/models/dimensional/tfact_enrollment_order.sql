{{ config(
    materialized='table'
) }}

-- Resolves which order produced each enrollment. xPRO enrollments carry a direct
-- order FK (tfact_enrollment.order_id), authoritative even after deferral to a
-- different course run or bundling into a program purchase; if that order hasn't
-- resolved yet (not fulfilled/refunded, or not synced to tfact_order), we fall back to
-- product-matching rather than dropping the enrollment. A null order_id on xPRO is
-- treated as definitive ("no purchase") and is NOT sent through that fallback. Every
-- other platform has no direct FK at the source at all, so their order is always
-- reconstructed by matching the enrollment's course/program against dim_product, then
-- to tfact_order — fragile under the same deferral/bundling scenarios, but the only
-- signal available for them.
-- Materialized as a full-table rebuild (not incremental): tfact_enrollment's own
-- incremental watermark tracks enrollment activity, not order-side changes (refunds,
-- price updates, fulfillment), so a table refresh keeps this current with tfact_order
-- without needing its own watermark logic.
-- Grain: one row per enrollment_key.
with enrollment as (
    select * from {{ ref('tfact_enrollment') }}
)

, product as (
    select * from {{ ref('dim_product') }}
    where is_current = true
)

, order_fact as (
    select * from {{ ref('tfact_order') }}
    where order_state in ('fulfilled', 'refunded')
)

, order_via_direct_fk as (
    select
        enrollment.enrollment_key
        , order_fact.order_key as order_fk
        , order_fact.order_id
        , order_fact.order_reference_number
        , order_fact.order_state
        , order_fact.order_updated_on
        , order_fact.discount_fk
        , order_fact.discount_type_fk
        , order_fact.line_price
        , order_fact.line_id
        -- the order is already known correct (direct FK); among its lines, prefer
        -- the course-run-specific match (for an accurate unit_price/discount), then a
        -- program-level match, falling back to any other line.
        , case
            when product.courserun_fk = enrollment.courserun_fk then 1
            when product.program_fk = enrollment.program_fk then 2
            else 3
        end as match_priority
    from enrollment
    inner join order_fact
        on
            enrollment.order_id = order_fact.order_id
            and enrollment.platform_fk = order_fact.platform_fk
    left join product
        on order_fact.product_fk = product.product_pk
    where enrollment.order_id is not null
)

-- An enrollment's courserun_fk/program_fk can each independently match a distinct
-- product (e.g. the course sold standalone AND bundled into a program the learner
-- actually purchased). Used as a fallback:
--  (a) for platforms with no direct order FK at all (order_id is always null there), or
--  (b) for xPRO when a direct order_id exists but didn't resolve above — e.g. the order
--      hasn't reached a fulfilled/refunded state yet, or hasn't landed in tfact_order
--      yet — so we attempt a best-effort reconstruction rather than silently dropping
--      the enrollment.
-- Deliberately NOT attempted when order_id is null for a platform that has a direct FK
-- mechanism (xPRO free/audit enrollments): a null order_id there is authoritative
-- ("genuinely no purchase"), and reconstructing via product-matching risks attaching an
-- unrelated real order the same user separately purchased for the same product.
, order_via_product as (
    select
        enrollment.enrollment_key
        , order_fact.order_key as order_fk
        , order_fact.order_id
        , order_fact.order_reference_number
        , order_fact.order_state
        , order_fact.order_updated_on
        , order_fact.discount_fk
        , order_fact.discount_type_fk
        , order_fact.line_price
        , order_fact.line_id
        , case when enrollment.courserun_fk = product.courserun_fk then 1 else 2 end as match_priority
    from enrollment
    inner join product
        on
            (enrollment.courserun_fk = product.courserun_fk
            or enrollment.program_fk = product.program_fk)
            and enrollment.platform_fk = product.platform_fk
    inner join order_fact
        on
            enrollment.user_fk = order_fact.user_fk
            and product.product_pk = order_fact.product_fk
            and enrollment.platform_fk = order_fact.platform_fk
    where
        not exists (
            select 1 from order_via_direct_fk as d where d.enrollment_key = enrollment.enrollment_key
        )
        and not (enrollment.platform = 'mitxpro' and enrollment.order_id is null)
)

select
    enrollment_key
    , order_fk
    , order_id
    , order_reference_number
    , order_state
    , order_updated_on
    , discount_fk
    , discount_type_fk
    , line_price
from (
    select
        *
        , row_number() over (
            partition by enrollment_key
            order by
                match_priority
                , order_updated_on desc nulls last
                -- order_updated_on is order-level and shared by every line on a
                -- multi-line order, so it alone can leave ties unresolved.
                , order_id desc nulls last
                , line_id desc nulls last
        ) as row_num
    from (
        select * from order_via_direct_fk
        union all
        select * from order_via_product
    )
)
where row_num = 1
