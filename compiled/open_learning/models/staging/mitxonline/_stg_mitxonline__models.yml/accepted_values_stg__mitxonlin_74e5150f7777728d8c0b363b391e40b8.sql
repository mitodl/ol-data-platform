with all_values as (

    select
        programrequirement_operator as value_field
        , count(*) as n_records

    from dev.main_staging.stg__mitxonline__app__postgres__courses_programrequirement
    group by programrequirement_operator

)

select *
from all_values
where value_field not in (
    'all_of', 'min_number_of'
)
