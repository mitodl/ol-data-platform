{% macro transform_gender_value(column_name) %}
        case
            when {{ column_name }} = 'm' then 'Male'
            when {{ column_name }} = 'f' then 'Female'
            when {{ column_name }} = 't' then 'Transgender'
            when {{ column_name }} = 'b' then 'Binary'
            when {{ column_name }} = 'nb' then 'Non-binary/non-conforming'
            when {{ column_name }} = 'o' then 'Other/Prefer Not to Say'
            else null
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
        --- the following two are no longer used, but there are still users' profiles with these values
        when {{ column_name }} = 'p_se' then 'Doctorate in science or engineering'
        when {{ column_name }} = 'p_oth' then 'Doctorate in another field'
        else null
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
