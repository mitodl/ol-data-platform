with all_values as (

    select
        platform as value_field
        , count(*) as n_records

    from dev.main_intermediate.int__mitx__users
    group by platform

)

select *
from all_values
where value_field not in (
    'Bootcamps', 'xPro', 'MITx Online', 'MicroMasters', 'edX.org'
)
