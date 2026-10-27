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

  program_completion_timestamp is NULL for "override" certificates (see
  __micromasters_program_certificates_non_dedp.sql's non_dedp_overides
  CTE, sourced from a hardcoded override list with no timestamp column at
  all) — those recipients earned a certificate outside the normal
  completion-tracking path, so there's no real completion time to report.
  last_modified falls back to current_timestamp for exactly those rows:
  it must never be NULL (breaks the not_null test and makes
  `since > last_modified` never match, so the row would never be picked
  up by an incremental pull) — current_timestamp just means this rare
  row gets re-upserted (a no-op) on every incremental run rather than
  being incrementally skippable, which is a fine tradeoff for a
  literally-one-row edge case.

  mitxonline_program_id is null upstream for every non-DEDP certificate:
  int__mitx__programs only links a MicroMasters program to a MITx Online one
  for DEDP. The edX.org programs have since moved to MITx Online, so the id is
  filled here from edxorg_to_mitxonline_program_mapping. It is not filled in
  int__micromasters__program_certificates because that model derives
  program_readable_id from mitxonline_program_id, and
  marts__micromasters_program_certificates exposes the result.
#}

with certificates as (
    select * from {{ ref('int__micromasters__program_certificates') }}
)

, program_mapping as (
    select * from {{ ref('edxorg_to_mitxonline_program_mapping') }}
)

, mitxonline_programs as (
    select * from {{ ref('int__mitxonline__programs') }}
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
    , coalesce(certificates.mitxonline_program_id, mitxonline_programs.program_id) as mitxonline_program_id
    , certificates.user_first_name
    , certificates.user_last_name
    , certificates.user_gender
    -- Cast intentional: profiles.ProgramCertificate.user_year_of_birth is a
    -- Django CharField (not an IntegerField) on the MIT Learn side.
    , cast(certificates.user_year_of_birth as varchar) as user_year_of_birth
    , certificates.user_country
    , certificates.user_address_state_or_territory
    , certificates.user_address_city
    , certificates.user_address_postal_code
    , certificates.user_street_address
    , certificates.program_completion_timestamp
    -- Null upstream for edX.org and MicroMasters certificates, which have no revocation
    , coalesce(certificates.programcertificate_is_revoked, false) as is_revoked
    , coalesce(
        {{ from_iso8601_timestamp('certificates.program_completion_timestamp') }}
        , {{ current_timestamptz() }}
    ) as last_modified
from certificates
left join program_mapping
    on certificates.program_title = program_mapping.edxorg_program_title
left join mitxonline_programs
    on program_mapping.mitxonline_program_readable_id = mitxonline_programs.program_readable_id
where certificates.program_certificate_hashed_id is not null
