{{ config(materialized="view") }}

with
    mm_program_certificates as (select * from {{ ref("stg__micromasters__app__postgres__grades_programcertificate") }}),
    programs as (select * from {{ ref("int__mitx__programs") }})

select
    programs.micromasters_program_id,
    programs.program_title,
    programs.mitxonline_program_id,
    {{ generate_hash_id("mm_program_certificates.programcertificate_hash") }} as program_certificate_hashed_id,
    mm_program_certificates.programcertificate_created_on as program_completion_timestamp,
    mm_program_certificates.user_id as user_micromasters_id
from mm_program_certificates
left join programs on mm_program_certificates.program_id = programs.micromasters_program_id
where programs.is_dedp_program = true
