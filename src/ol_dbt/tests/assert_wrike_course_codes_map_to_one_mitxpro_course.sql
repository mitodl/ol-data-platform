{{ config(error_if='>0') }}

-- A Wrike course code that more than one xPro course holds would give its Emeritus and Global
-- Alumni runs more than one current row in dim_course_run. Returns each such code.
select
    wrike_course_code
    , count(*) as mitxpro_course_count
from ({{ wrike_course_codes_of_mitxpro_courses() }}) as mappings
group by wrike_course_code
having count(*) > 1
