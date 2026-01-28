{{ config(
    materialized='table'
) }}

-- Seed table or hardcoded reference data
select 1 as payment_method_pk, 'credit_card' as payment_method_code, 'Credit Card' as payment_method_name, 'card' as payment_method_type, 'cybersource' as payment_method_provider
union all
select 2, 'paypal', 'PayPal', 'wallet', 'paypal'
union all
select 3, 'wire_transfer', 'Wire Transfer', 'bank_transfer', null
union all
select 4, 'voucher', 'Voucher/Bulk Payment', 'voucher', null
