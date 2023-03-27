{% macro transform_gender_value(column_name) %}
        case
            when {{ column_name }} = 'm' then 'Male'
            when {{ column_name }} = 'f' then 'Female'
            when {{ column_name }} = 't' then 'Transgender'
            when {{ column_name }} = 'nb' then 'Non-binary/non-conforming'
            when {{ column_name }} = 'o' then 'Other/Prefer Not to Say'
            else {{ column_name }}
        end
{% endmacro %}

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

{% macro transform_company_size_value(column_name='company_size') %}
    case
        when {{ column_name }} = 1 then 'Small/Start-up (1+ employees)'
        when {{ column_name }} = 9 then 'Small/Home office (1-9 employees)'
        when {{ column_name }} = 99 then 'Small (10-99 employees)'
        when {{ column_name }} = 999 then 'Small to medium-sized (100-999 employees)'
        when {{ column_name }} = 9999 then 'Medium-sized (1000-9999 employees)'
        when {{ column_name }} = 10000 then 'Large Enterprise (10,000+ employees)'
        when {{ column_name }} = 0 then 'Other (N/A or Don''t know)'
        else cast({{ column_name }} as varchar)
    end
{% endmacro %}

{% macro transform_years_experience_value(column_name='years_experience') %}
   case
        when {{ column_name }} = 2 then 'Less than 2 years'
        when {{ column_name }} = 5 then '2-5 years'
        when {{ column_name }} = 10 then '6 - 10 years'
        when {{ column_name }} = 15 then '11 - 15 years'
        when {{ column_name }} = 20 then '16 - 20 years'
        when {{ column_name }} = 21 then 'More than 20 years'
        when {{ column_name }} = 0 then 'Prefer not to say'
        else cast({{ column_name }} as varchar)
    end
{% endmacro %}
