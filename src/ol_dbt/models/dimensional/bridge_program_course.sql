{{ config(
    materialized='table'
) }}

-- Program requirements define which courses belong to which programs
with micromasters_course_keys as (
    select
        course_id
        , course_edx_key as course_readable_id
    from {{ ref('stg__micromasters__app__postgres__courses_course') }}
)

, mitxonline_program_requirements as (
    select
        program_id
        , course_id
        , cast(null as varchar) as course_readable_id
        , true as is_required
        , 'mitxonline' as platform
        , 'mitxonline' as platform_code
    from {{ ref('int__mitxonline__program_requirements') }}
)

, mitxpro_program_requirements as (
    select
        program_id
        , course_id
        , cast(null as varchar) as course_readable_id
        , true as is_required
        , 'mitxpro' as platform
        , 'mitxpro' as platform_code
    from {{ ref('int__mitxpro__coursesinprogram') }}
)

, micromasters_requirements as (
    select
        r.program_id
        , cast(null as integer) as course_id  -- MicroMasters uses readable_id path
        , ck.course_readable_id
        , true as is_required
        , 'micromasters' as platform
        , 'micromasters' as platform_code
    from {{ ref('int__micromasters__program_requirements') }} r
    inner join micromasters_course_keys ck on r.course_id = ck.course_id
)

, combined_requirements as (
    select * from mitxonline_program_requirements
    union all
    select * from mitxpro_program_requirements
    union all
    select * from micromasters_requirements
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
        on (
            -- Standard path: integer source_id + platform match
            (combined_requirements.platform_code != 'micromasters'
                and combined_requirements.course_id = dim_course.source_id
                and combined_requirements.platform_code = dim_course.primary_platform)
            -- MicroMasters path: courses are edxorg rows in dim_course; match on readable_id
            or (combined_requirements.platform_code = 'micromasters'
                and combined_requirements.course_readable_id = dim_course.course_readable_id)
        )
)

select
    program_fk
    , course_fk
    , is_required
    , course_order
from bridge
