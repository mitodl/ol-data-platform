{{ config(
    materialized='table'
) }}

-- Map courses to departments (many-to-many)
-- Note: OCW courses not yet in dim_course (Phase 1-2), so OCW omitted here
with mitxonline_course_departments as (
    select
        course_id
        , coursedepartment_name as department_name
        , 'mitxonline' as platform
    from {{ ref('int__mitxonline__course_to_departments') }}
)

, combined_course_departments as (
    select * from mitxonline_course_departments
)

-- Join to dimensions to get FKs
, dim_course as (
    select course_pk, source_id, primary_platform
    from {{ ref('dim_course') }}
    where is_current = true
)

, dim_department as (
    select department_pk, department_name, primary_platform
    from {{ ref('dim_department') }}
)

, bridge as (
    select
        dim_course.course_pk as course_fk
        , dim_department.department_pk as department_fk
    from combined_course_departments
    left join dim_course
        on combined_course_departments.course_id = dim_course.source_id
        and combined_course_departments.platform = dim_course.primary_platform
    left join dim_department
        on combined_course_departments.department_name = dim_department.department_name
        and combined_course_departments.platform = dim_department.primary_platform
)

-- Include only fully-resolved rows; unresolved FKs indicate dim coverage gaps
select distinct
    course_fk
    , department_fk
from bridge
where course_fk is not null
    and department_fk is not null
