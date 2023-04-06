with all_values as (

    select
        flexiblepriceapplication_status as value_field
        , count(*) as n_records

    from dev.main_staging.stg__mitxonline__app__postgres__flexiblepricing_flexiblepriceapplication
    group by flexiblepriceapplication_status

)

select *
from all_values
where value_field not in (
    'approved', 'auto-approved', 'created', 'pending-manual-approval', 'denied', 'reset'
)
