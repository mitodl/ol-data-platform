-- This overrides the grant SQL to apply only to ROLEs since that is the requirement for
-- Starburst Galaxy's permissions system.

{%- macro trino__get_grant_sql(relation, privilege, grantees) -%}
    GRANT {{ privilege }} ON {{ relation }} TO ROLE {{ adapter.quote(grantees[0]) }}
{%- endmacro %}
