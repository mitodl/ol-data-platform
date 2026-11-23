-- is_current ranks program certificates by certificate ID, so a revoked certificate and
-- its reissue can both be current. A valid one ranks first, so a program reads as revoked
-- only when no valid certificate is left
with certificates as (
    select
        platform
        , user_fk
        , program_fk
        , certificate_uuid
        , certificate_is_revoked
        , certificate_issued_on
        , row_number() over (
            partition by platform, user_fk, program_fk
            order by certificate_is_revoked asc nulls last, certificate_issued_on desc nulls last, certificate_id desc
        ) as row_num
    from {{ ref('tfact_certificate') }}
    where platform in ('mitxpro', 'mitxonline') and certificate_scope = 'program' and is_current = true
)

, xpro_compared as (
    select
        dim_user.mitxpro_application_user_id as source_user_id
        , dim_user.mitxonline_application_user_id as user_mitxonline_id
        -- The xPRO login email, not dim_user's email from the newest platform
        , lower(mitxpro_users.user_email) as user_email
        , xpro_programs.program_title
        , xpro_programs.program_readable_id
        , cast(mitxonline_programs.source_id as bigint) as program_id
        , xpro_certificate.certificate_uuid
        , xpro_certificate.certificate_is_revoked
        , xpro_certificate.certificate_issued_on
        , case
            when
                mitxonline_certificate.user_fk is null
                and not xpro_certificate.certificate_is_revoked
                and dim_user.user_is_active_on_mitxpro
                then 'new'
            -- A revoked certificate or inactive account is not migrated
            when mitxonline_certificate.user_fk is null then null
            when ({{ is_distinct_from('xpro_certificate.certificate_is_revoked', 'mitxonline_certificate.certificate_is_revoked') }})
                then 'certificate_changed'
        end as migration_status
    from certificates as xpro_certificate
    inner join {{ ref('dim_user') }} as dim_user
        on xpro_certificate.user_fk = dim_user.user_pk
    inner join {{ ref('int__mitxpro__users') }} as mitxpro_users
        on dim_user.mitxpro_application_user_id = mitxpro_users.user_id
    inner join {{ ref('dim_program') }} as xpro_programs
        on
            xpro_certificate.program_fk = xpro_programs.program_pk
            and xpro_programs.platform_code = 'mitxpro'
    left join {{ ref('dim_program') }} as mitxonline_programs
        on
            xpro_programs.program_readable_id = mitxonline_programs.program_readable_id
            and mitxonline_programs.platform_code = 'mitxonline'
    left join certificates as mitxonline_certificate
        on
            xpro_certificate.user_fk = mitxonline_certificate.user_fk
            and mitxonline_programs.program_pk = mitxonline_certificate.program_fk
            and mitxonline_certificate.platform = 'mitxonline'
            and mitxonline_certificate.row_num = 1
    where xpro_certificate.platform = 'mitxpro' and xpro_certificate.row_num = 1
)

, xpro_program_certificates as (
    select
        'xpro' as source
        , migration_status
        , source_user_id
        , user_mitxonline_id
        , user_email
        , program_title
        , program_readable_id
        , program_id
        , certificate_uuid as programcertificate_uuid
        , certificate_is_revoked as programcertificate_is_revoked
        , certificate_issued_on as program_certificate_issued_on
    from xpro_compared
    where migration_status is not null
)

-- Add each new source as a CTE and union all it here
select
    source
    , migration_status
    , source_user_id
    , user_mitxonline_id
    , user_email
    , program_title
    , program_readable_id
    , program_id
    , programcertificate_uuid
    , programcertificate_is_revoked
    , program_certificate_issued_on
from xpro_program_certificates
