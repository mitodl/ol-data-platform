-- This overrides the grant SQL to apply only to ROLEs since that is the requirement for
-- Starburst Galaxy's permissions system.
{%- macro trino__get_grant_sql(relation, privilege, grantees) -%}
    grant {{ privilege }} on {{ relation }} to role {{ adapter.quote(grantees[0]) }}
{%- endmacro %}

-- This overrides the revoke SQL to apply only to ROLEs since that is the requirement for
-- Starburst Galaxy's permissions system.'
{%- macro trino__get_revoke_sql(relation, privilege, grantees) -%}
    revoke {{ privilege }} on {{ relation }} from role {{ adapter.quote(grantees[0]) }}
{%- endmacro %}
