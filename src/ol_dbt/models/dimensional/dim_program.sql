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
            cms_programpage_first_published_on as first_published_date,  -- Keep as VARCHAR
            cast(null as varchar) as enrollment_start_date,
            cast(null as varchar) as enrollment_end_date
        from {{ ref("int__mitxpro__programs") }}
    ),
    micromasters_programs as (
        select
            program_id as source_id,
            cast(program_id as varchar) as program_readable_id,  -- micromasters doesn't have readable_id
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
    with_platform_fk as (
        select
            combined.*,
            cast(null as integer) as platform_fk,  -- dim_platform not in Phase 1-2
            {{ safe_parse_iso8601_date("first_published_date") }} as published_date,
            cast(
                date_format({{ safe_parse_iso8601_date("first_published_date") }}, '%Y%m%d') as integer
            ) as published_date_key
        from combined
    )

select {{ dbt_utils.generate_surrogate_key(["source_id", "platform_readable_id"]) }} as program_pk, *
from with_platform_fk
