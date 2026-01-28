{{ config(
    materialized='table'
) }}

-- Program requirements define which courses belong to which programs
with mitxonline_program_requirements as (
    select
        program_id
        , course_id
        , true as is_required
    from {{ ref('int__mitxonline__program_requirements') }}
)

, mitxpro_program_requirements as (
    select
        program_id
        , course_id
        , true as is_required
    from {{ ref('int__mitxpro__coursesinprogram') }}
)

, combined_requirements as (
    select * from mitxonline_program_requirements
    union all
    select * from mitxpro_program_requirements
)

-- Join to dimensions to get FKs
, dim_program as (
    select program_pk, program_readable_id, source_id, platform_readable_id
    from {{ ref('dim_program') }}
)

, dim_course as (
    select course_pk, course_readable_id, source_id, primary_platform
    from {{ ref('dim_course') }}
    where is_current = true
)

, bridge as (
    select
        dim_program.program_pk as program_fk
        , dim_course.course_pk as course_fk
        , combined_requirements.is_required
        , ROW_NUMBER() OVER (
            PARTITION BY dim_program.program_pk, dim_course.course_pk
            ORDER BY combined_requirements.program_id
          ) as course_order
    from combined_requirements
    inner join dim_program
        on combined_requirements.program_id = dim_program.source_id
    inner join dim_course
        on combined_requirements.course_id = dim_course.source_id
)

select
    program_fk
    , course_fk
    , is_required
    , course_order
from bridge
