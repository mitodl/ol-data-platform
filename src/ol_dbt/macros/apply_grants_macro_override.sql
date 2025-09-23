{% macro apply_grants(relation, grant_config, should_revoke) %}
    {% if target.name == "production" %}
        {#- - Only apply grants in production schema --#}
        {{ return(adapter.dispatch("apply_grants", "dbt")(relation, grant_config, should_revoke)) }}
    {% else %}
    {#- - Bypass code-based grant application in non-prod targets. --#}
    {#- - This is so that we don't accidentally expose test tables to end-consumers of data --#}
    {% endif %}
{% endmacro %}
