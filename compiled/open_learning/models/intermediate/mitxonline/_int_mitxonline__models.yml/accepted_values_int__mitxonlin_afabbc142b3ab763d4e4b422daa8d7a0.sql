with all_values as (

    select
        programrequirement_operator as value_field
        , count(*) as n_records

    from dev.main_intermediate.int__mitxonline__programrequirements
    group by programrequirement_operator

)

select *
from all_values
where value_field not in (
    'all_of', 'min_number_of'
)
