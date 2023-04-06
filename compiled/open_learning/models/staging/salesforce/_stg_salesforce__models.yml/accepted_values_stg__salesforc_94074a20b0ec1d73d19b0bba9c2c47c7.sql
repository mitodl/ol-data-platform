with all_values as (

    select
        opportunity_business_type as value_field
        , count(*) as n_records

    from dev.main_staging.stg__salesforce__opportunity
    group by opportunity_business_type

)

select *
from all_values
where value_field not in (
    'B2C', 'B2B', 'B2X'
)
