select 1 as discount_type_pk, 'percentage' as discount_type_code, 'Percentage Discount' as discount_type_name
union all
select 2 as discount_type_pk, 'fixed_amount' as discount_type_code, 'Fixed Amount Discount' as discount_type_name
union all
select 3 as discount_type_pk, 'free' as discount_type_code, 'Free (100% Discount)' as discount_type_name
