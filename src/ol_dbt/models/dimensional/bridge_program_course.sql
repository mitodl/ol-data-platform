{{ config(
    materialized='table'
) }}

-- Program requirements define which courses belong to which programs
with mitxonline_program_requirements as (
    select
        program_id
        , course_id
        , true as is_required
        , 'mitxonline' as platform
        , 'mitxonline' as platform_code
    from {{ ref('int__mitxonline__program_requirements') }}
)

, mitxpro_program_requirements as (
    select
        program_id
        , course_id
        , true as is_required
        , 'mitxpro' as platform
        , 'mitxpro' as platform_code
    from {{ ref('int__mitxpro__coursesinprogram') }}
)

, combined_requirements as (
    select * from mitxonline_program_requirements
    union all
    select * from mitxpro_program_requirements
)

-- Join to dimensions to get FKs
, dim_program as (
    select program_pk, program_readable_id, source_id, platform_readable_id, platform_code
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
        -- course_order is set to 1 as a placeholder. The source systems (MITx Online,
        -- xPro, MicroMasters) do not expose a canonical ordering of courses within a
        -- program, so a meaningful sort order cannot be derived without additional
        -- business logic or manual curation.
        , 1 as course_order
    from combined_requirements
    inner join dim_program
        on combined_requirements.program_id = dim_program.source_id
        and combined_requirements.platform_code = dim_program.platform_code
    inner join dim_course
        on combined_requirements.course_id = dim_course.source_id
        and combined_requirements.platform_code = dim_course.primary_platform
)

select
    program_fk
    , course_fk
    , is_required
    , course_order
from bridge
