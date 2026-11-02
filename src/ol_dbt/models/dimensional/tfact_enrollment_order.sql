-- Resolves which order produced each enrollment. xPRO enrollments carry a direct
-- order FK (tfact_enrollment.order_id), authoritative even after deferral to a
-- different course run or bundling into a program purchase; if that order hasn't
-- resolved yet (not fulfilled/refunded, or not synced to tfact_order), we fall back to
-- product-matching rather than dropping the enrollment. A null order_id on xPRO is
-- treated as definitive ("no purchase") and is NOT sent through that fallback. Every
-- other platform has no direct FK at the source at all, so their order is always
-- reconstructed by matching the enrollment's course/program against dim_product, then
-- to tfact_order — fragile under the same deferral/bundling scenarios, but the only
-- signal available for them. MITx Online's paid course run record corrects the deferral
-- case: a deferred enrollment takes its order from the run it was deferred from.
-- Materialized as a full-table rebuild (not incremental): tfact_enrollment's own
-- incremental watermark tracks enrollment activity, not order-side changes (refunds,
-- price updates, fulfillment), so a table refresh keeps this current with tfact_order
-- without needing its own watermark logic.
-- Grain: one row per enrollment_key.
{%- set order_columns %}
        , order_fact.order_key as order_fk
        , order_fact.order_id
        , order_fact.order_reference_number
        , order_fact.order_state
        , order_fact.order_updated_on
        , order_fact.discount_fk
        , order_fact.discount_type_fk
        , order_fact.line_price
        , order_fact.line_id
        , order_fact.platform
        , product.product_type
{%- endset %}
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

, course_run as (
    select * from {{ ref('dim_course_run') }}
    where is_current = true
)

-- A deferral moves this record to the new run. The order's line keeps the run it was
-- bought for, so this is the only link from the order to the new enrollment.
, mitxonline_paid_course_run as (
    select distinct
        paid_course_run.order_id
        , course_run.courserun_pk as courserun_fk
    from {{ ref('stg__mitxonline__app__postgres__courses_paidcourserun') }} as paid_course_run
    inner join course_run
        on
            paid_course_run.courserun_id = course_run.source_id
            and course_run.platform = 'mitxonline'
)

, mitxonline_deferred_orders as (
    select
        deferred_order.order_id
        , course_run.courserun_pk as courserun_fk
    from {{ ref('int__mitxonline__deferred_course_run_orders') }} as deferred_order
    inner join course_run
        on
            deferred_order.courserun_id = course_run.source_id
            and course_run.platform = 'mitxonline'
)

, order_via_direct_fk as (
    select
        enrollment.enrollment_key
        {{ order_columns }}
        , cast(null as varchar) as deferred_from_courserun_readable_id
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

, order_via_paid_course_run as (
    select
        enrollment.enrollment_key
        {{ order_columns }}
        , case
            when product.courserun_fk != enrollment.courserun_fk then line_run.courserun_readable_id
        end as deferred_from_courserun_readable_id
        , case when product.courserun_fk = enrollment.courserun_fk then 1 else 2 end as match_priority
    from enrollment
    inner join mitxonline_paid_course_run
        on enrollment.courserun_fk = mitxonline_paid_course_run.courserun_fk
    inner join order_fact
        on
            mitxonline_paid_course_run.order_id = order_fact.order_id
            and enrollment.user_fk = order_fact.user_fk
            and enrollment.platform_fk = order_fact.platform_fk
    left join product
        on order_fact.product_fk = product.product_pk
    left join course_run as line_run
        on product.courserun_fk = line_run.courserun_pk
    where enrollment.platform = 'mitxonline'
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
        {{ order_columns }}
        , cast(null as varchar) as deferred_from_courserun_readable_id
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
    left join mitxonline_deferred_orders
        on
            order_fact.order_id = mitxonline_deferred_orders.order_id
            and enrollment.courserun_fk = mitxonline_deferred_orders.courserun_fk
    where
        not exists (
            select 1 from order_via_direct_fk as d where d.enrollment_key = enrollment.enrollment_key
        )
        and not (enrollment.platform = 'mitxpro' and enrollment.order_id is null)
        -- Once a deferral moves the order to another run, the enrollment it was bought
        -- for no longer holds it, unless the app itself kept that enrollment verified.
        -- This is the app's mode: a deferred order doesn't verify a DEDP run, see
        -- int__mitxonline__courserunenrollments.
        and (
            mitxonline_deferred_orders.order_id is null
            or coalesce(enrollment.enrollment_mode, '') = 'verified'
        )
)

, resolved_orders as (
    select
        *
        , row_number() over (
            partition by enrollment_key
            order by
                match_priority asc
                , order_updated_on desc nulls last
                -- order_updated_on is order-level and shared by every line on a
                -- multi-line order, so it alone can leave ties unresolved.
                , order_id desc nulls last
                , line_id desc nulls last
        ) as row_num
    from (
        select * from order_via_direct_fk
        union all
        select * from order_via_paid_course_run
        union all
        select * from order_via_product
    )
)

, mitxonline_program_course_run_orders as (
    select * from {{ ref('int__mitxonline__program_course_run_orders') }}
)

, program_order_discounts as (
    select
        order_id
        , platform
        , max(discount_fk) as program_discount_fk
    from order_fact
    group by order_id, platform
)

, enrollment_orders as (
    select
        resolved_orders.enrollment_key
        , resolved_orders.order_fk
        , resolved_orders.order_id
        , resolved_orders.order_reference_number
        , resolved_orders.order_state
        , resolved_orders.order_updated_on
        , resolved_orders.discount_fk
        , resolved_orders.discount_type_fk
        , resolved_orders.line_price
        , resolved_orders.platform
        , resolved_orders.deferred_from_courserun_readable_id
        -- xPRO enrollments from a program purchase carry the program order itself; MITx Online
        -- enrollments carry a $0 course run order, so their program order is matched.
        , coalesce(
            case
                when resolved_orders.product_type in ('program', 'courses_programrun')
                    then resolved_orders.order_id
            end
            , mitxonline_program_course_run_orders.program_order_id
        ) as program_order_id
    from resolved_orders
    left join mitxonline_program_course_run_orders
        on
            resolved_orders.platform = 'mitxonline'
            and resolved_orders.line_id = cast(mitxonline_program_course_run_orders.line_id as varchar)
    where resolved_orders.row_num = 1
)

select
    enrollment_orders.enrollment_key
    , enrollment_orders.order_fk
    , enrollment_orders.order_id
    , enrollment_orders.order_reference_number
    , enrollment_orders.order_state
    , enrollment_orders.order_updated_on
    , enrollment_orders.discount_fk
    , enrollment_orders.discount_type_fk
    , enrollment_orders.line_price
    , enrollment_orders.deferred_from_courserun_readable_id
    , enrollment_orders.program_order_id
    , program_order_discounts.program_discount_fk
from enrollment_orders
left join program_order_discounts
    on
        enrollment_orders.program_order_id = program_order_discounts.order_id
        and enrollment_orders.platform = program_order_discounts.platform
