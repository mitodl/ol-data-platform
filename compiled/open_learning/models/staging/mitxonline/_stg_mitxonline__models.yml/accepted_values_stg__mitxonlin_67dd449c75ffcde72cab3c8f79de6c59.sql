with all_values as (

    select
        programrequirement_node_type as value_field
        , count(*) as n_records

    from dev.main_staging.stg__mitxonline__app__postgres__courses_programrequirement
    group by programrequirement_node_type

)

select *
from all_values
where value_field not in (
    'program_root', 'course', 'operator'
)
