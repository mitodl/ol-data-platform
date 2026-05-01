with edxorg_program_certificates as (
    select * from (
        select
            *
            , row_number() over (
                partition by user_id, program_title
                order by program_certificate_awarded_on
            ) as row_num
        from {{ ref('int__edxorg__mitx_program_certificates') }}
        where user_username not like 'retired__user%'
    )
    where row_num = 1
)

, mitxonline_programs as (
    select
        program_id
        , program_title
        , program_readable_id
    from {{ ref('int__mitxonline__programs') }}
)

, mitx__users as (
    select * from {{ ref('int__mitx__users') }}
)

, mitxonline_program_certificates as (
    select
        user_id
        , program_id
        , user_email
    from {{ ref('int__mitxonline__program_certificates') }}
)

select
    edxorg_program_certificates.user_id as user_edxorg_id
    , coalesce(mitx_users_by_email.user_mitxonline_id, mitx__users.user_mitxonline_id) as user_mitxonline_id
    , coalesce(mitx__users.user_mitxonline_email, mitx__users.user_edxorg_email) as user_email
    , edxorg_program_certificates.program_title
    , edxorg_program_certificates.program_type
    , mitxonline_programs.program_id
    , mitxonline_programs.program_readable_id
    , edxorg_program_certificates.program_certificate_awarded_on as program_certificate_issued_on
from edxorg_program_certificates
left join mitxonline_programs
    on lower(mitxonline_programs.program_title) = lower(edxorg_program_certificates.program_title)
left join mitx__users
    on edxorg_program_certificates.user_id = mitx__users.user_edxorg_id
left join mitx__users as mitx_users_by_email
    on lower(mitx__users.user_edxorg_email) = lower(mitx_users_by_email.user_mitxonline_email)
left join mitxonline_program_certificates
    on coalesce(mitx_users_by_email.user_mitxonline_id, mitx__users.user_mitxonline_id)
        = mitxonline_program_certificates.user_id
    and mitxonline_programs.program_id = mitxonline_program_certificates.program_id
left join mitxonline_program_certificates as mitxonline_program_certificates_by_email
    on lower(mitx__users.user_mitxonline_email) = lower(mitxonline_program_certificates_by_email.user_email)
    and mitxonline_programs.program_id = mitxonline_program_certificates_by_email.program_id
where
    --- Exclude certificates already present on MITx Online (matched by user ID or email)
    mitxonline_program_certificates.user_id is null
    and mitxonline_program_certificates_by_email.user_email is null
