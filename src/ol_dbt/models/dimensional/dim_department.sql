{{ config(
    materialized='table'
) }}

-- Consolidate departments from platforms that have this metadata
-- Use production intermediate tables for OCW since not in tmacey schema
with ocw_departments as (
    select distinct
        course_department_number as department_number
        , course_department_name as department_name
        , 'ocw' as platform
    from ol_data_lake_production.ol_warehouse_production_intermediate.int__ocw__course_departments
    where course_department_name is not null
)

, mitxonline_departments as (
    select distinct
        cast(null as varchar) as department_number
        , coursedepartment_name as department_name
        , '{{ var("mitxonline") }}' as platform
    from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__course_to_departments
    where coursedepartment_name is not null
)

, combined_departments as (
    select * from ocw_departments
    union all
    select * from mitxonline_departments
)

-- Deduplicate by department_name
, deduped_departments as (
    select
        department_name
        , max(department_number) as department_number  -- OCW typically has this
        , min(platform) as primary_platform
    from combined_departments
    group by department_name
)

select
    {{ dbt_utils.generate_surrogate_key(['department_name']) }} as department_pk
    , department_name
    , department_number
    , primary_platform
from deduped_departments
