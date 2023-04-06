with all_values as (

    select
        discount_source as value_field
        , count(*) as n_records

    from dev.main_staging.stg__mitxonline__app__postgres__ecommerce_discount
    group by discount_source

)

select *
from all_values
where value_field not in (
    'marketing', 'sales', 'financial-assistance', 'customer-support', 'staff', 'legacy'
)
