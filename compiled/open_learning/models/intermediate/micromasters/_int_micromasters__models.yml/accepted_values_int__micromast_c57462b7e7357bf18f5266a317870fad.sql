with all_values as (

    select
        programrequirement_type as value_field
        , count(*) as n_records

    from dev.main_intermediate.int__micromasters__program_requirements
    group by programrequirement_type

)

select *
from all_values
where value_field not in (
    'Core', 'Elective'
)
