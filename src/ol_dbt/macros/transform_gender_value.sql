{% macro transform_gender_value(column_name) %}
        case
            when {{ column_name }} = 'm' then 'Male'
            when {{ column_name }} = 'f' then 'Female'
            when {{ column_name }} = 'o' then 'Other/Prefer Not to Say'
            else {{ column_name }}
        end
{% endmacro %}
