{{ config(
    materialized='table'
) }}

-- Seed table or hardcoded reference data.
-- payment_method_code is the canonical warehouse code used for joins in tfact_payment.
-- Cybersource's raw req_payment_method field uses different values (e.g. 'card' for credit card);
-- normalization to these codes is applied in the intermediate transaction/receipt models.
-- NOTE: refund transactions and zero-dollar payments have no req_payment_method value;
-- tfact_payment.payment_method_fk is NULL for those rows and that is expected/valid.
select 1 as payment_method_pk, 'credit_card' as payment_method_code, 'Credit Card' as payment_method_name, 'card' as payment_method_type, 'cybersource' as payment_method_provider
union all
select 2, 'paypal', 'PayPal', 'wallet', 'paypal'
union all
select 3, 'wire_transfer', 'Wire Transfer', 'bank_transfer', null
union all
select 4, 'voucher', 'Voucher/Bulk Payment', 'voucher', null
