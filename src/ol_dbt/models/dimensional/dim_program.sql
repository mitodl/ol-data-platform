{{ config(materialized="table", unique_key="program_pk") }}

with
    mitxonline_programs as (
        select
            program_id as source_id,
            program_readable_id,
            program_name,
            program_title,
            program_type,
            program_track,
            program_certification_type,
            program_is_dedp,
            program_is_micromasters,
            program_availability,
            program_price,  -- VARCHAR type with $ prefix
            program_length,
            program_effort,
            program_description,
            program_what_you_learn,
            program_prerequisites,
            program_is_live as is_active,
            false as is_external,
            cast(null as varchar) as partner_platform_name,
            'MITx Online' as platform_readable_id,
            'mitxonline' as platform_code,
            program_page_first_published_on as first_published_date,  -- Keep as VARCHAR
            -- Temporal attributes directly in dimension
            cast(null as varchar) as enrollment_start_date,  -- Not time-bound in MITx Online
            cast(null as varchar) as enrollment_end_date
        from {{ ref("int__mitxonline__programs") }}
    ),
    mitxpro_programs as (
        select
            program_id as source_id,
            program_readable_id,
            program_title as program_name,
            program_title,
            'Professional' as program_type,  -- xPro is professional development
            cast(null as varchar) as program_track,
            cast(null as varchar) as program_certification_type,
            false as program_is_dedp,
            false as program_is_micromasters,
            'anytime' as program_availability,
            cast(null as varchar) as program_price,
            cast(null as varchar) as program_length,
            cms_programpage_time_commitment as program_effort,
            cms_programpage_description as program_description,
            cast(null as varchar) as program_what_you_learn,
            cast(null as varchar) as program_prerequisites,
            program_is_live as is_active,
            program_is_external,
            platform_name as partner_platform_name,
            case when program_is_external then 'xPRO ' || platform_name else 'xPro' end as platform_readable_id,
            'mitxpro' as platform_code,
            cms_programpage_first_published_on as first_published_date,  -- Keep as VARCHAR
            cast(null as varchar) as enrollment_start_date,
            cast(null as varchar) as enrollment_end_date
        from {{ ref("int__mitxpro__programs") }}
    ),
    micromasters_programs as (
        select
            program_id as source_id,
            {{ generate_micromasters_program_readable_id('program_id', 'program_title') }} as program_readable_id,
            program_title as program_name,
            program_title,
            'MicroMasters' as program_type,
            cast(null as varchar) as program_track,  -- micromasters doesn't have track
            'MicroMasters Credential' as program_certification_type,
            false as program_is_dedp,  -- micromasters doesn't have dedp flag
            true as program_is_micromasters,
            'dated' as program_availability,
            cast(null as varchar) as program_price,  -- micromasters doesn't have price
            cast(null as varchar) as program_length,  -- micromasters doesn't have length
            cast(null as varchar) as program_effort,
            program_description,
            cast(null as varchar) as program_what_you_learn,  -- micromasters doesn't have this
            cast(null as varchar) as program_prerequisites,  -- micromasters doesn't have this
            program_is_live as is_active,
            false as is_external,
            cast(null as varchar) as partner_platform_name,
            'MITx Online' as platform_readable_id,
            'micromasters' as platform_code,
            cast(null as varchar) as first_published_date,
            cast(null as varchar) as enrollment_start_date,
            cast(null as varchar) as enrollment_end_date
        from {{ ref("int__micromasters__programs") }}
    ),
    combined as (
        select *
        from mitxonline_programs
        union all
        select *
        from mitxpro_programs
        union all
        select *
        from micromasters_programs
    ),
    dim_platform_lookup as (
        select platform_pk, platform_readable_id
        from {{ ref('dim_platform') }}
    ),
    with_platform_fk as (
        select
            combined.*,
            dim_platform_lookup.platform_pk as platform_fk,
            {{ safe_parse_iso8601_date("first_published_date") }} as published_date,
            {{ iso8601_to_date_key('first_published_date') }} as published_date_key
        from combined
        left join dim_platform_lookup
            on combined.platform_code = dim_platform_lookup.platform_readable_id
    )

select {{ dbt_utils.generate_surrogate_key(["source_id", "platform_code"]) }} as program_pk, *
from with_platform_fk
