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
        , user_id
        , transaction_amount
        , transaction_type
        , transaction_status
        , payment_method
        , transaction_created_on
        , '{{ var("mitxonline") }}' as platform
    from {{ ref('int__mitxonline__ecommerce_transaction') }}
)

, mitxpro_payments as (
    select
        transaction_id as payment_id
        , order_id
        , user_id
        , transaction_amount
        , transaction_type
        , transaction_status
        , payment_method
        , transaction_created_on
        , '{{ var("mitxpro") }}' as platform
    from {{ ref('int__mitxpro__ecommerce_transaction') }}
)

, combined_payments as (
    select * from mitxonline_payments
    union all
    select * from mitxpro_payments
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

, dim_payment_method as (
    select payment_method_pk, payment_method_code
    from {{ ref('dim_payment_method') }}
)

, payments_with_fks as (
    select
        combined_payments.*
        , dim_user.user_pk as user_fk
        , dim_platform.platform_pk as platform_fk
        , dim_payment_method.payment_method_pk as payment_method_fk
        , cast(format_datetime(transaction_created_on, 'yyyyMMdd') as integer) as payment_date_key
    from combined_payments
    left join dim_platform on combined_payments.platform = dim_platform.platform_readable_id
    left join dim_user
        on
            (combined_payments.platform = '{{ var("mitxonline") }}' and combined_payments.user_id = dim_user.mitxonline_user_id)
            or (combined_payments.platform = '{{ var("mitxpro") }}' and combined_payments.user_id = dim_user.mitxpro_user_id)
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
        , payment_method_fk
        , transaction_amount
        , transaction_type
        , transaction_status
    from payments_with_fks

    {% if is_incremental() %}
    where transaction_created_on > (select max(transaction_created_on) from {{ this }})
    {% endif %}
)

select * from final
