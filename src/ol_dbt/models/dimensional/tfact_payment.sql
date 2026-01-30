{{ config(
    materialized='incremental',
    unique_key='payment_key',
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
        , '{{ var("mitxonline") }}' as platform
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
        , '{{ var("mitxpro") }}' as platform
    from {{ ref('int__mitxpro__ecommerce_receipt') }}
)

, combined_payments as (
    select * from mitxonline_payments
    union all
    select * from mitxpro_payments
)

-- Join to dimensions for FKs
, dim_payment_method as (
    select payment_method_pk, payment_method_code
    from {{ ref('dim_payment_method') }}
)

, payments_with_fks as (
    select
        combined_payments.*
        , cast(null as integer) as platform_fk  -- dim_platform not in Phase 1-2
        , dim_payment_method.payment_method_pk as payment_method_fk
        , case when transaction_created_on is not null
            then cast(date_format(date_parse(substr(transaction_created_on, 1, 19), '%Y-%m-%dT%H:%i:%s'), '%Y%m%d') as integer)
            else null end as payment_date_key
    from combined_payments
    left join dim_payment_method on combined_payments.payment_method = dim_payment_method.payment_method_code
)

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
        , platform
        , payment_method_fk
        , transaction_amount
        , transaction_type
        , transaction_status
        , transaction_created_on
    from payments_with_fks

    {% if is_incremental() %}
    where transaction_created_on > (select max(transaction_created_on) from {{ this }})
    {% endif %}
)

select * from final
