create table ol_data_lake_production.ol_warehouse_production_staging.stg__micromasters__app__user_program_certificate_override_list__dbt_tmp

as (
    select * from (
        values
        (11479768, 1)
        , (7752757, 4)
        , (25033349, 4)
        , (1586443, 4)
        , (26106900, 1)
    ) AS overwrite_list (user_edxorg_id, micromasters_program_id) -- noqa: L010,L025
);
