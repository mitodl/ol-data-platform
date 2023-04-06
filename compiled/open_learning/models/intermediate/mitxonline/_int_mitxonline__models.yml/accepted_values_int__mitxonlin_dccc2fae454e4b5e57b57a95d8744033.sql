with all_values as (

    select
        courseware_type as value_field
        , count(*) as n_records

    from dev.main_intermediate.int__mitxonline__flexiblepricing_flexiblepriceapplication
    group by courseware_type

)

select *
from all_values
where value_field not in (
    'course', 'program'
)
