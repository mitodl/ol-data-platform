{% macro transform_education_value(column_name) %}
    case
        when {{ column_name }} = 'p' then 'Doctorate'
        when {{ column_name }} = 'm' then 'Master''s or professional degree'
        when {{ column_name }} = 'b' then 'Bachelor''s degree'
        when {{ column_name }} = 'a' then 'Associate degree'
        when {{ column_name }} = 'hs' then 'Secondary/high school'
        when {{ column_name }} = 'jhs' then 'Junior secondary/junior high/middle school'
        when {{ column_name }} = 'el' then 'Elementary/primary school'
        when {{ column_name }} = 'none' then 'No formal education'
        when {{ column_name }} = 'other' or {{ column_name }} = 'o' then 'Other education'
        --- the following two are no longer used, but there are still user's profiles have these values
        when {{ column_name }} = 'p_se' then 'Doctorate in science or engineering'
        when {{ column_name }} = 'p_oth' then 'Doctorate in another field'
        else {{ column_name }}
    end
{% endmacro %}
