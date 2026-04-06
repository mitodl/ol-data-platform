{{ config(
    materialized='table'
) }}

-- Consolidate departments from platforms that have this metadata
with ocw_departments as (
    select distinct
        course_department_number as department_number
        , course_department_name as department_name
        , 'ocw' as platform
    from {{ ref('int__ocw__course_departments') }}
    where course_department_name is not null
)

, mitxonline_departments as (
    select distinct
        cast(null as varchar) as department_number
        , coursedepartment_name as department_name
        , 'mitxonline' as platform
    from {{ ref('int__mitxonline__course_to_departments') }}
    where coursedepartment_name is not null
)

, combined_departments as (
    select * from ocw_departments
    union all
    select * from mitxonline_departments
)

-- Keep per-platform rows — same department name from different platforms are separate records.
, deduped_departments as (
    select
        department_name
        , max(department_number) as department_number  -- OCW typically has this
        , platform as primary_platform
    from combined_departments
    group by department_name, platform
)

select
    {{ dbt_utils.generate_surrogate_key(['department_name', 'primary_platform']) }} as department_pk
    , department_name
    , department_number
    , primary_platform
from deduped_departments
