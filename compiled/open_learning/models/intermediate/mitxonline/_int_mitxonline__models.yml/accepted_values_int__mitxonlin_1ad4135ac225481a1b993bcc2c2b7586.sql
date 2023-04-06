with all_values as (

    select
        user_company_size as value_field
        , count(*) as n_records

    from dev.main_intermediate.int__mitxonline__users
    group by user_company_size

)

select *
from all_values
where value_field not in (
    'Small/Start-up (1+ employees)'
    , 'Small/Home office (1-9 employees)'
    , 'Small (10-99 employees)'
    , 'Small to medium-sized (100-999 employees)'
    , 'Medium-sized (1000-9999 employees)'
    , 'Large Enterprise (10,000+ employees)'
    , 'Other (N/A or Don''t know)'
)
