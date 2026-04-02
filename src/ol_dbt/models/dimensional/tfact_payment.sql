{{ config(
    materialized='incremental',
    unique_key='payment_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- Consolidate payments from all platforms
with mitxonline_payments as (
    select
        transaction_id as payment_id
        , order_id
        , null as user_id
        , transaction_amount
        , transaction_type
        , transaction_status
        , transaction_payment_method as payment_method
        , transaction_created_on
        , 'mitxonline' as platform
    from {{ ref('int__mitxonline__ecommerce_transaction') }}
)

, mitxpro_payments as (
    select
        receipt_id as payment_id
        , order_id
        , null as user_id
        , receipt_payment_amount as transaction_amount
        , receipt_transaction_type as transaction_type
        , receipt_transaction_status as transaction_status
        , receipt_payment_method as payment_method
        , receipt_created_on as transaction_created_on
        , 'mitxpro' as platform
    from {{ ref('int__mitxpro__ecommerce_receipt') }}
)

, combined_payments as (
    select * from mitxonline_payments
    union all
    select * from mitxpro_payments
)

-- Bridge from payment/receipt → order → user (payment sources carry null user_id by design).
-- Use staging order tables (order grain) rather than the intermediate models, which join
-- to lines and would fan out for multi-line orders.
, order_user_lookup as (
    select
        order_id
        , order_purchaser_user_id as user_id
        , 'mitxonline' as platform
    from {{ ref('stg__mitxonline__app__postgres__ecommerce_order') }}
    union all
    select
        order_id
        , order_purchaser_user_id as user_id
        , 'mitxpro' as platform
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_order') }}
)

-- Join to dimensions for FKs
, user_lookup as (
    select
        user_pk
        , mitxonline_application_user_id
        , mitxpro_application_user_id
    from {{ ref('dim_user') }}
    where user_pk is not null
)

, dim_platform_lookup as (
    select platform_pk, platform_readable_id
    from {{ ref('dim_platform') }}
)

, dim_payment_method as (
    select payment_method_pk, payment_method_code
    from {{ ref('dim_payment_method') }}
)

, payments_with_fks as (
    select
        combined_payments.*
        , coalesce(
            case when combined_payments.platform = 'mitxonline'
                then ul_mitxonline.user_pk
            end,
            case when combined_payments.platform = 'mitxpro'
                then ul_mitxpro.user_pk
            end
        ) as user_fk
        , dim_platform_lookup.platform_pk as platform_fk
        , dim_payment_method.payment_method_pk as payment_method_fk
        , {{ iso8601_to_date_key('transaction_created_on') }} as payment_date_key
    from combined_payments
    left join order_user_lookup as oul
        on combined_payments.order_id = oul.order_id
        and combined_payments.platform = oul.platform
    left join user_lookup as ul_mitxonline
        on combined_payments.platform = 'mitxonline'
        and oul.user_id = ul_mitxonline.mitxonline_application_user_id
    left join user_lookup as ul_mitxpro
        on combined_payments.platform = 'mitxpro'
        and oul.user_id = ul_mitxpro.mitxpro_application_user_id
    left join dim_platform_lookup
        on combined_payments.platform = dim_platform_lookup.platform_readable_id
    left join dim_payment_method on combined_payments.payment_method = dim_payment_method.payment_method_code
)

{% if is_incremental() %}
-- Pre-compute per-platform watermarks to avoid the correlated subquery anti-pattern.
-- Without this, Trino fans out to (left rows × target rows per platform) intermediate
-- rows for the AssignUniqueId + LeftJoin + StreamingAggregate execution pattern.
, incremental_watermarks as (
    select platform, max(transaction_created_on) as max_created_on
    from {{ this }}
    group by platform
)
{% endif %}

, final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'cast(payment_id as varchar)',
            'platform'
        ]) }} as payment_key
        , payment_id
        , order_id
        , payment_date_key
        , user_fk
        , platform_fk
        , pwf.platform
        , payment_method_fk
        , transaction_amount
        , transaction_type
        , transaction_status
        , transaction_created_on
    from payments_with_fks as pwf

    {% if is_incremental() %}
    -- left join preserves payments from platforms not yet in the target table
    left join incremental_watermarks w on w.platform = pwf.platform
    where (
        w.max_created_on is null  -- platform not yet in target, include all
        or pwf.transaction_created_on > w.max_created_on
        or pwf.transaction_created_on is null
    )
    {% endif %}
)

select * from final
