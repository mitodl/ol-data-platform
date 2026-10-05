{#
    Open edX profile codes and the labels they are reported as. The transform
    macros and the accepted_values lists are both generated from these, so a
    label added here is accepted by the tests without a second edit.

    The same lists also test columns that skip the transform because the app
    already stores the label (e.g. MITx Online's highest_education), so a label
    only those apps emit still needs an entry here.
#}
{% macro gender_code_labels() %}
    {% do return({
        'm': 'Male',
        'f': 'Female',
        't': 'Transgender',
        'b': 'Binary',
        'nb': 'Non-binary/non-conforming',
        'o': 'Other/Prefer Not to Say',
    }) %}
{% endmacro %}

{# p_se and p_oth are no longer offered, but profiles still carry them. #}
{% macro education_code_labels() %}
    {% do return({
        'p': 'Doctorate',
        'm': "Master's or professional degree",
        'b': "Bachelor's degree",
        'a': 'Associate degree',
        'hs': 'Secondary/high school',
        'jhs': 'Junior secondary/junior high/middle school',
        'el': 'Elementary/primary school',
        'none': 'No formal education',
        'other': 'Other education',
        'o': 'Other education',
        'p_se': 'Doctorate in science or engineering',
        'p_oth': 'Doctorate in another field',
    }) %}
{% endmacro %}

{% macro code_labels_case(column_name, code_labels) %}
    case
        {%- for code, label in code_labels.items() %}
        when {{ column_name }} = '{{ code }}' then '{{ label | replace("'", "''") }}'
        {%- endfor %}
        else null
    end
{% endmacro %}

{#
    Values for an accepted_values test, which quotes each one without escaping
    it. The empty string is accepted alongside the labels, as it was in the
    project vars these lists replace.
#}
{% macro code_labels_accepted_values(code_labels) %}
    {% set labels = [] %}
    {% for label in code_labels.values() | unique %}
        {% do labels.append(label | replace("'", "''")) %}
    {% endfor %}
    {% do return(labels + ['']) %}
{% endmacro %}

{% macro transform_gender_value(column_name) %}
    {{ code_labels_case(column_name, gender_code_labels()) }}
{% endmacro %}

{% macro gender_values() %}
    {% do return(code_labels_accepted_values(gender_code_labels())) %}
{% endmacro %}

{% macro transform_education_value(column_name) %}
    {{ code_labels_case(column_name, education_code_labels()) }}
{% endmacro %}

{% macro highest_education_values() %}
    {% do return(code_labels_accepted_values(education_code_labels())) %}
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

---- https://github.com/mitodl/ocw-studio/blob/master/static/js/resources/departments.json
{% macro transform_ocw_department_number(column_name='department_number') %}
   case
        when {{ column_name }} = '5' then 'Chemistry'
        when {{ column_name }} = '20' then 'Biological Engineering'
        when {{ column_name }} = '16' then 'Aeronautics and Astronautics'
        when {{ column_name }} = '21G' then 'Global Studies and Languages'
        when {{ column_name }} = '18' then 'Mathematics'
        when {{ column_name }} = 'HST' then 'Health Sciences and Technology'
        when {{ column_name }} = 'EC' then 'Edgerton Center'
        when {{ column_name }} = 'WGS' then 'Women''s and Gender Studies'
        when {{ column_name }} = '11' then 'Urban Studies and Planning'
        when {{ column_name }} = '15' then 'Sloan School of Management'
        when {{ column_name }} = '21A' then 'Anthropology'
        when {{ column_name }} = 'ESD' then 'Engineering Systems Division'
        when {{ column_name }} = '10' then 'Chemical Engineering'
        when {{ column_name }} = '12' then 'Earth, Atmospheric, and Planetary Sciences'
        when {{ column_name }} = '21M' then 'Music and Theater Arts'
        when {{ column_name }} = 'PE' then 'Athletics, Physical Education and Recreation'
        when {{ column_name }} = '4' then 'Architecture'
        when {{ column_name }} = 'ES' then 'Experimental Study Group'
        when {{ column_name }} = '21L' then 'Literature'
        when {{ column_name }} = '2' then 'Mechanical Engineering'
        when {{ column_name }} = '6' then 'Electrical Engineering and Computer Science'
        when {{ column_name }} = '3' then 'Materials Science and Engineering'
        when {{ column_name }} = '21H' then 'History'
        when {{ column_name }} = '24' then 'Linguistics and Philosophy'
        when {{ column_name }} = 'IDS' then 'Institute for Data, Systems, and Society'
        when {{ column_name }} = '7' then 'Biology'
        when {{ column_name }} = '1' then 'Civil and Environmental Engineering'
        when {{ column_name }} = 'CMS-W' then 'Comparative Media Studies/Writing'
        when {{ column_name }} = '22' then 'Nuclear Science and Engineering'
        when {{ column_name }} = '14' then 'Economics'
        when {{ column_name }} = 'CC' then 'Concourse'
        when {{ column_name }} = '9' then 'Brain and Cognitive Sciences'
        when {{ column_name }} = 'STS' then 'Science, Technology, and Society'
        when {{ column_name }} = '17' then 'Political Science'
        when {{ column_name }} = 'MAS' then 'Media Arts and Sciences'
        when {{ column_name }} = '8' then 'Physics'
        when {{ column_name }} = 'RES' then 'Supplemental Resources'
        else {{ column_name }}
    end
{% endmacro %}


--- https://github.com/mitodl/mit-learn/blob/main/learning_resources/constants.py#L189-L227
{% macro transform_edx_department_number(column_name='department_number') %}
   case
       when {{ column_name }} = '1' then 'Civil and Environmental Engineering'
       when {{ column_name }} = '2' then 'Mechanical Engineering'
       when {{ column_name }} = '3' then 'Materials Science and Engineering'
       when {{ column_name }} = '4' then 'Architecture'
       when {{ column_name }} = '5' then 'Chemistry'
       when {{ column_name }} = '6' then 'Electrical Engineering and Computer Science'
       when {{ column_name }} = '7' then 'Biology'
       when {{ column_name }} = '8' then 'Physics'
       when {{ column_name }} = '9' then 'Brain and Cognitive Sciences'
       when {{ column_name }} = '10' then 'Chemical Engineering'
       when {{ column_name }} = '11' then 'Urban Studies and Planning'
       when {{ column_name }} = '12' then 'Earth, Atmospheric, and Planetary Sciences'
       when {{ column_name }} = '14' then 'Economics'
       when {{ column_name }} = '15' then 'Management'
       when {{ column_name }} = '16' then 'Aeronautics and Astronautics'
       when {{ column_name }} = '17' then 'Political Science'
       when {{ column_name }} = '18' then 'Mathematics'
       when {{ column_name }} = '20' then 'Biological Engineering'
       when {{ column_name }} = '21A' then 'Anthropology'
       when {{ column_name }} = '21G' then 'Global Languages'
       when {{ column_name }} = '21H' then 'History'
       when {{ column_name }} = '21L' then 'Literature'
       when {{ column_name }} = '21M' then 'Music and Theater Arts'
       when {{ column_name }} = '22' then 'Nuclear Science and Engineering'
       when {{ column_name }} = '24' then 'Linguistics and Philosophy'
       when {{ column_name }} = 'CC' then 'Concourse'
       when {{ column_name }} = 'CMS-W' then 'Comparative Media Studies/Writing'
       when {{ column_name }} = 'EC' then 'Edgerton Center'
       when {{ column_name }} = 'ES' then 'Experimental Study Group'
       when {{ column_name }} = 'ESD' then 'Engineering Systems Division'
       when {{ column_name }} = 'HST' then 'Medical Engineering and Science'
       when {{ column_name }} = 'IDS' then 'Data, Systems, and Society'
       when {{ column_name }} = 'MAS' then 'Media Arts and Sciences'
       when {{ column_name }} = 'PE' then 'Athletics, Physical Education and Recreation'
       when {{ column_name }} = 'SP' then 'Special Programs'
       when {{ column_name }} = 'STS' then 'Science, Technology, and Society'
       when {{ column_name }} = 'WGS' then 'Women''s and Gender Studies'
       else null
   end
{% endmacro %}
