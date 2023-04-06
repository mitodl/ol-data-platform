with all_values as (

    select
        opportunity_type as value_field
        , count(*) as n_records

    from dev.main_intermediate.int__salesforce__opportunity
    group by opportunity_type

)

select *
from all_values
where value_field not in (
    'Existing Business', 'Expansion Business', 'New Business'
)
