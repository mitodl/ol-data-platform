{{ config(
    materialized='table',
    unique_key='program_pk'
) }}

with mitxonline_programs as (
    select
        program_id as source_id
        , program_readable_id
        , program_name
        , program_title
        , program_type
        , program_track
        , program_certification_type
        , program_is_dedp
        , program_is_micromasters
        , program_availability
        , program_price
        , program_length
        , program_effort
        , program_description
        , program_what_you_learn
        , program_prerequisites
        , program_is_live as is_active
        , false as is_external
        , null as partner_platform_name
        , 'MITx Online' as platform_readable_id
        , program_page_first_published_on as first_published_date
        -- Temporal attributes directly in dimension
        , null as enrollment_start_date  -- Not time-bound in MITx Online
        , null as enrollment_end_date
    from {{ ref('int__mitxonline__programs') }}
)

, mitxpro_programs as (
    select
        program_id as source_id
        , program_readable_id
        , program_title as program_name
        , program_title
        , 'Professional' as program_type  -- xPro is professional development
        , null as program_track
        , null as certification_type
        , false as program_is_dedp
        , false as program_is_micromasters
        , 'anytime' as program_availability
        , null as program_price
        , null as program_length
        , cms_programpage_time_commitment as program_effort
        , cms_programpage_description as program_description
        , null as program_what_you_learn
        , null as program_prerequisites
        , program_is_live as is_active
        , program_is_external
        , platform_name as partner_platform_name
        , CASE
            WHEN program_is_external THEN CONCAT('xPRO ', platform_name)
            ELSE 'xPro'
          END as platform_readable_id
        , cms_programpage_first_published_on as first_published_date
        , null as enrollment_start_date
        , null as enrollment_end_date
    from {{ ref('int__mitxpro__programs') }}
)

, micromasters_programs as (
    select
        program_id as source_id
        , cast(program_id as varchar) as program_readable_id  -- micromasters doesn't have readable_id
        , program_title as program_name
        , program_title
        , 'MicroMasters' as program_type
        , cast(null as varchar) as program_track  -- micromasters doesn't have track
        , 'MicroMasters Credential' as certification_type
        , false as program_is_dedp  -- micromasters doesn't have dedp flag
        , true as program_is_micromasters
        , 'dated' as program_availability
        , cast(null as double) as program_price  -- micromasters doesn't have price
        , cast(null as varchar) as program_length  -- micromasters doesn't have length
        , cast(null as varchar) as program_effort
        , program_description
        , cast(null as varchar) as program_what_you_learn  -- micromasters doesn't have this
        , cast(null as varchar) as program_prerequisites  -- micromasters doesn't have this
        , program_is_live as is_active
        , false as is_external
        , cast(null as varchar) as partner_platform_name
        , 'MITx Online' as platform_readable_id
        , cast(null as date) as first_published_date
        , cast(null as date) as enrollment_start_date
        , cast(null as date) as enrollment_end_date
    from {{ ref('int__micromasters__programs') }}
)

, combined as (
    select * from mitxonline_programs
    union all
    select * from mitxpro_programs
    union all
    select * from micromasters_programs
)

, with_platform_fk as (
    select
        combined.*
        , dim_platform.platform_pk as platform_fk
        , {{ safe_parse_iso8601_date('first_published_date') }} as published_date
        , CAST(
            FORMAT_DATETIME(
                {{ safe_parse_iso8601_date('first_published_date') }},
                'yyyyMMdd'
            ) AS INTEGER
          ) as published_date_key
    from combined
    inner join {{ ref('dim_platform') }} as dim_platform
        on combined.platform_readable_id = dim_platform.platform_readable_id
)

select
    {{ dbt_utils.generate_surrogate_key([
        'source_id',
        'platform_readable_id'
    ]) }} as program_pk
    , *
from with_platform_fk
