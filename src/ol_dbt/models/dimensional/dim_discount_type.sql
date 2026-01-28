{{ config(
    materialized='table'
) }}

select 1 as discount_type_pk, 'percentage' as discount_type_code, 'Percentage Discount' as discount_type_name
union all
select 2, 'fixed_amount', 'Fixed Amount Discount'
union all
select 3, 'free', 'Free (100% Discount)'
