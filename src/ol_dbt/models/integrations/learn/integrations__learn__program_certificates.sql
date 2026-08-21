{#
  integrations__learn__program_certificates
  Exposes MicroMasters + MITx Online program certificates for MIT Learn's
  warehouse-pull ETL, replacing the Hightouch sync into MIT Learn's
  external.programcertificate table (mitodl/hq#12954). Column set mirrors
  the fields profiles.ProgramCertificate actually stores in MIT Learn,
  which is wider than the "core" catalog contract (includes recipient
  name/contact fields, not just program identity).

  NOTE: this model intentionally does not follow the readable_id/title/
  etl_source-enum shape in docs/learn_marts_contract.md — that contract is
  scoped to catalog resources (courses/programs) feeding MIT Learn's
  LearningResource model. A program certificate is a fact record feeding a
  different model (profiles.ProgramCertificate), keyed by record_hash, not
  a catalog entity. last_modified is still exposed since MIT Learn's
  warehouse-pull machinery (BaseWarehouseETLTask/iter_rows) depends on it
  generically for incremental pulls, but no genuine "last modified" signal
  exists in the source certificate lineage (a certificate is issued once
  and essentially never revised) — program_completion_timestamp is reused
  as the closest available proxy.
#}

with certificates as (
    select * from {{ ref('int__micromasters__program_certificates') }}
)

select
    certificates.program_certificate_hashed_id as record_hash
    , certificates.program_title
    , certificates.user_full_name
    , coalesce(certificates.user_email, '')     as user_email
    , certificates.user_edxorg_id
    , certificates.user_edxorg_username
    , certificates.user_mitxonline_username
    , certificates.micromasters_program_id
    , certificates.mitxonline_program_id
    , certificates.user_first_name
    , certificates.user_last_name
    , certificates.user_gender
    , cast(certificates.user_year_of_birth as varchar) as user_year_of_birth
    , certificates.user_country
    , certificates.user_address_state_or_territory
    , certificates.user_address_city
    , certificates.user_address_postal_code
    , certificates.user_street_address
    , certificates.program_completion_timestamp
    , certificates.program_completion_timestamp as last_modified
from certificates
where certificates.program_certificate_hashed_id is not null
