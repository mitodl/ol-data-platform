{%- set street = 'mitxpro_users.user_street_address' -%}
{%- set street_1 = dbt.split_part(street, codepoint_char(10), 1) -%}

with xpro_enrolled_users as (
    select source_user_id
    from {{ ref('mitxonline_migration_course_enrollments') }}
    where source = 'xpro'
    union
    select source_user_id
    from {{ ref('mitxonline_migration_program_certificates') }}
    where source = 'xpro'
)

, xpro_users as (
    select
        'xpro' as source
        , dim_user.mitxpro_application_user_id as source_user_id
        -- The xPRO profile, not dim_user's values from the newest platform
        , lower(mitxpro_users.user_email) as user_email
        , mitxpro_users.user_full_name
        , mitxpro_users.user_gender
        , mitxpro_users.user_birth_year
        , mitxpro_users.user_address_country
        , mitxpro_users.user_address_state_or_territory as user_address_state
        , mitxpro_users.user_address_city
        , mitxpro_users.user_address_postal_code
        -- xPRO keeps up to five street lines joined by newlines; MITx Online has two
        , {{ street_1 }} as user_address_street_1
        , nullif(replace(substr({{ street }}, {{ char_length(street_1) }} + 2), {{ codepoint_char(10) }}, ', '), '')
            as user_address_street_2
    from {{ ref('dim_user') }} as dim_user
    inner join xpro_enrolled_users
        on dim_user.mitxpro_application_user_id = xpro_enrolled_users.source_user_id
    inner join {{ ref('int__mitxpro__users') }} as mitxpro_users
        on dim_user.mitxpro_application_user_id = mitxpro_users.user_id
    where dim_user.mitxonline_application_user_id is null
)

-- Add each new source as a CTE and union all it here
select
    source
    , source_user_id
    , user_email
    , user_full_name
    , user_gender
    , user_birth_year
    , user_address_country
    , user_address_state
    , user_address_city
    , user_address_postal_code
    , user_address_street_1
    , user_address_street_2
from xpro_users
