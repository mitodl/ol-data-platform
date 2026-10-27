with program_certificates_dedp_from_micromasters as (
    select *
    from {{ ref('__micromasters_program_certificates_dedp_from_micromasters') }}
)

, program_certificates_dedp_from_mitxonline as (
    select *
    from {{ ref('__micromasters_program_certificates_dedp_from_mitxonline') }}
)

, program_certificates_non_dedp as (
    select *
    from {{ ref('__micromasters_program_certificates_non_dedp') }}
)

, mitx_users as (
    select * from {{ ref('int__mitx__users') }}
)

, mitxonline_programs as (
    select * from {{ ref('int__mitxonline__programs') }}
)

-- MITx Online is the source of DEDP certificates. It holds copies of most certificates MicroMasters
-- issued, so a MicroMasters certificate is kept only when MITx Online has no copy of it.
, dedp_micromasters as (
    select
        program_certificates_dedp_from_micromasters.program_certificate_hashed_id
        , program_certificates_dedp_from_micromasters.mitxonline_program_id
        , program_certificates_dedp_from_micromasters.program_completion_timestamp
        , mitx_users.user_mitxonline_id
        , lower(mitx_users.user_micromasters_email) as user_email
    from program_certificates_dedp_from_micromasters
    left join mitx_users
        on program_certificates_dedp_from_micromasters.user_micromasters_id = mitx_users.user_micromasters_id
)

, dedp_mitxonline as (
    select
        program_certificates_dedp_from_mitxonline.program_certificate_hashed_id
        , program_certificates_dedp_from_mitxonline.mitxonline_program_id
        , program_certificates_dedp_from_mitxonline.program_completion_timestamp
        , program_certificates_dedp_from_mitxonline.user_mitxonline_id
        , lower(mitx_users.user_mitxonline_email) as user_email
    from program_certificates_dedp_from_mitxonline
    left join mitx_users
        on program_certificates_dedp_from_mitxonline.user_mitxonline_id = mitx_users.user_mitxonline_id
)

-- A copy is first matched by user and program, with the MicroMasters account linked to the MITx Online
-- account through int__mitx__users.
, dedp_matches_by_user as (
    select
        dedp_micromasters.program_certificate_hashed_id as micromasters_program_certificate_hashed_id
        , dedp_mitxonline.program_certificate_hashed_id as mitxonline_program_certificate_hashed_id
    from dedp_micromasters
    inner join dedp_mitxonline
        on
            dedp_micromasters.user_mitxonline_id = dedp_mitxonline.user_mitxonline_id
            and dedp_micromasters.mitxonline_program_id = dedp_mitxonline.mitxonline_program_id
)

-- Fallback for certificates with no match by user: int__mitx__users links some MicroMasters accounts to a
-- different MITx Online account, or to none, while the copy sits on a MITx Online account with the
-- learner's MicroMasters email. The migration kept each certificate's creation time, so the same email,
-- program and exact creation time identify the copy. Without this the copy would be a second certificate
-- for the learner in MIT Learn.
, dedp_matches_by_email as (
    select
        dedp_micromasters.program_certificate_hashed_id as micromasters_program_certificate_hashed_id
        , dedp_mitxonline.program_certificate_hashed_id as mitxonline_program_certificate_hashed_id
    from dedp_micromasters
    inner join dedp_mitxonline
        on
            dedp_micromasters.user_email = dedp_mitxonline.user_email
            and dedp_micromasters.mitxonline_program_id = dedp_mitxonline.mitxonline_program_id
            and dedp_micromasters.program_completion_timestamp = dedp_mitxonline.program_completion_timestamp
    left join dedp_matches_by_user as micromasters_matched
        on
            dedp_micromasters.program_certificate_hashed_id
            = micromasters_matched.micromasters_program_certificate_hashed_id
    left join dedp_matches_by_user as mitxonline_matched
        on
            dedp_mitxonline.program_certificate_hashed_id
            = mitxonline_matched.mitxonline_program_certificate_hashed_id
    where
        micromasters_matched.micromasters_program_certificate_hashed_id is null
        and mitxonline_matched.mitxonline_program_certificate_hashed_id is null
)

, dedp_certificate_matches as (
    select * from dedp_matches_by_user
    union all
    select * from dedp_matches_by_email
)

, report as (
    select
        program_certificates_dedp_from_micromasters.program_certificate_hashed_id
        , program_certificates_dedp_from_micromasters.program_title
        , program_certificates_dedp_from_micromasters.micromasters_program_id
        , program_certificates_dedp_from_micromasters.mitxonline_program_id
        , program_certificates_dedp_from_micromasters.program_completion_timestamp
        , mitx_users.user_edxorg_id
        , mitx_users.user_edxorg_username
        , mitx_users.user_mitxonline_username
        , mitx_users.user_micromasters_email as user_email
        , mitx_users.user_first_name
        , mitx_users.user_last_name
        , mitx_users.user_full_name
        , mitx_users.user_gender
        , mitx_users.user_birth_year as user_year_of_birth
        , mitx_users.user_address_country as user_country
        , mitx_users.user_address_state as user_address_state_or_territory
        , mitx_users.user_address_city
        , mitx_users.user_address_postal_code
        , mitx_users.user_street_address
        , true as program_is_dedp
        , program_certificates_dedp_from_micromasters.programcertificate_is_revoked
    from program_certificates_dedp_from_micromasters
    left join mitx_users
        on program_certificates_dedp_from_micromasters.user_micromasters_id = mitx_users.user_micromasters_id
    left join dedp_certificate_matches
        on
            program_certificates_dedp_from_micromasters.program_certificate_hashed_id
            = dedp_certificate_matches.micromasters_program_certificate_hashed_id
    where dedp_certificate_matches.micromasters_program_certificate_hashed_id is null

    union all

    -- MIT Learn upserts certificates on this hash and never deletes one, and its program letters
    -- reference the certificate row. A MITx Online copy of a MicroMasters certificate keeps the
    -- MicroMasters hash, so the row Learn already has is updated and not duplicated.
    select
        coalesce(
            dedp_certificate_matches.micromasters_program_certificate_hashed_id
            , program_certificates_dedp_from_mitxonline.program_certificate_hashed_id
        ) as program_certificate_hashed_id
        , program_certificates_dedp_from_mitxonline.program_title
        , program_certificates_dedp_from_mitxonline.micromasters_program_id
        , program_certificates_dedp_from_mitxonline.mitxonline_program_id
        , program_certificates_dedp_from_mitxonline.program_completion_timestamp
        , mitx_users.user_edxorg_id
        , mitx_users.user_edxorg_username
        , mitx_users.user_mitxonline_username
        , mitx_users.user_mitxonline_email as user_email
        , mitx_users.user_first_name
        , mitx_users.user_last_name
        , mitx_users.user_full_name
        , mitx_users.user_gender
        , mitx_users.user_birth_year as user_year_of_birth
        , mitx_users.user_address_country as user_country
        , mitx_users.user_address_state as user_address_state_or_territory
        , mitx_users.user_address_city
        , mitx_users.user_address_postal_code
        , mitx_users.user_street_address
        , true as program_is_dedp
        , program_certificates_dedp_from_mitxonline.programcertificate_is_revoked
    from program_certificates_dedp_from_mitxonline
    left join mitx_users
        on program_certificates_dedp_from_mitxonline.user_mitxonline_id = mitx_users.user_mitxonline_id
    left join dedp_certificate_matches
        on
            program_certificates_dedp_from_mitxonline.program_certificate_hashed_id
            = dedp_certificate_matches.mitxonline_program_certificate_hashed_id

    union all

    select
        program_certificates_non_dedp.program_certificate_hashed_id
        , program_certificates_non_dedp.program_title
        , program_certificates_non_dedp.micromasters_program_id
        , program_certificates_non_dedp.mitxonline_program_id
        , program_certificates_non_dedp.program_completion_timestamp
        , mitx_users.user_edxorg_id
        , mitx_users.user_edxorg_username
        , mitx_users.user_mitxonline_username
        , mitx_users.user_edxorg_email as user_email
        , mitx_users.user_first_name
        , mitx_users.user_last_name
        , mitx_users.user_full_name
        , mitx_users.user_gender
        , mitx_users.user_birth_year as user_year_of_birth
        , mitx_users.user_address_country as user_country
        , mitx_users.user_address_state as user_address_state_or_territory
        , mitx_users.user_address_city
        , mitx_users.user_address_postal_code
        , mitx_users.user_street_address
        , false as program_is_dedp
        , program_certificates_non_dedp.programcertificate_is_revoked
    from program_certificates_non_dedp
    left join mitx_users
        on program_certificates_non_dedp.user_edxorg_id = mitx_users.user_edxorg_id
)

select
    report.program_certificate_hashed_id
    , report.user_edxorg_username
    , report.user_mitxonline_username
    , report.user_email
    , report.program_title
    , report.micromasters_program_id
    , report.mitxonline_program_id
    , report.user_edxorg_id
    , report.program_completion_timestamp
    , report.user_gender
    , report.user_first_name
    , report.user_last_name
    , report.user_full_name
    , report.user_year_of_birth
    , report.user_country
    , report.user_address_postal_code
    , report.user_address_city
    , report.user_street_address
    , report.user_address_state_or_territory
    , report.program_is_dedp
    , report.programcertificate_is_revoked
    , if(
        mitxonline_programs.program_id is not null
        , mitxonline_programs.program_readable_id
        , {{ generate_micromasters_program_readable_id('report.micromasters_program_id','report.program_title') }}
    ) as program_readable_id
from report
left join mitxonline_programs
    on report.mitxonline_program_id = mitxonline_programs.program_id
order by report.program_completion_timestamp desc
