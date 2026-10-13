{#
    Fails for each non-null value that is not a JSON array. unnest_json_array yields
    no rows for such a value, so a model unnesting the column drops the row silently.
#}
{% test json_array(model, column_name) %}

select {{ column_name }}
from {{ model }}
where
    {{ column_name }} is not null
    and {{ try_parse_json_array(column_name) }} is null

{% endtest %}
