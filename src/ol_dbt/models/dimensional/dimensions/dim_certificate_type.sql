{{ config(
    materialized='table'
) }}

select 1 as certificate_type_pk, 'verified' as certificate_type_code, 'Verified Certificate' as certificate_type_name, true as certificate_requires_id_verification
union all
select 2, 'professional', 'Professional Certificate', true
union all
select 3, 'completion', 'Certificate of Completion', false
union all
select 4, 'audit', 'Audit Certificate', false
union all
select 5, 'micromasters', 'MicroMasters Credential', true
