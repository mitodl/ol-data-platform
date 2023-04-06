select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select user_edxorg_id
    from
        ol_data_lake_production.ol_warehouse_production_staging.stg__micromasters__app__user_program_certificate_override_list
    where user_edxorg_id is null




) as dbt_internal_test
