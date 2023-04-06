with all_values as (

    select
        courserun_platform as value_field
        , count(*) as n_records

    from dev.main_staging.stg__edxorg__bigquery__mitx_user_email_opt_in
    group by courserun_platform

)

select *
from all_values
where value_field not in (
    'Bootcamps', 'xPro', 'MITx Online', 'MicroMasters', 'edX.org'
)
