{% macro apply_grants(relation, grant_config, should_revoke) %}
    {#-- Only the production targets apply grants, so test tables built
         elsewhere never reach end consumers. --#}
    {% if target.name in var("grant_targets") %}
        {{ return(adapter.dispatch("apply_grants", "dbt")(
            relation, engine_grant_config(grant_config), should_revoke
        )) }}
    {% endif %}
{% endmacro %}

{#--
    Galaxy and StarRocks have different roles, and both build from one grants
    config until the cutover, so the config names the roles of both. Each
    engine grants only the roles var('grant_roles') lists for it and drops the
    rest. A name listed for neither engine is a typo and fails the model.
--#}
{% macro engine_grant_config(grant_config) %}
    {% set grant_roles = var("grant_roles") %}
    {% set engine_roles = grant_roles[target.type] %}
    {% set known_roles = grant_roles.values() | sum(start=[]) %}
    {% set engine_config = {} %}
    {% for privilege, grantees in (grant_config or {}).items() %}
        {% set unknown = grantees | reject("in", known_roles) | list %}
        {% if unknown %}
            {{ exceptions.raise_compiler_error(
                "grants." ~ privilege ~ " names " ~ unknown
                ~ ", which no engine in var('grant_roles') has"
            ) }}
        {% endif %}
        {% do engine_config.update({privilege: grantees | select("in", engine_roles) | list}) %}
    {% endfor %}
    {{ return(engine_config) }}
{% endmacro %}
