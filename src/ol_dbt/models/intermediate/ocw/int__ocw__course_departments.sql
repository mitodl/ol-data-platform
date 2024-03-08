with websitecontents as (
    select * from {{ ref('stg__ocw__studio__postgres__websites_websitecontent') }}
)

select
    websitecontents.website_uuid as course_uuid
    , course_department_number -- noqa
    , {{ transform_ocw_department_number('course_department_number') }} as course_department_name
from websitecontents
cross join unnest(cast(json_parse(course_department_numbers) as array(varchar))) as t(course_department_number) -- noqa
where websitecontents.websitecontent_type = 'sitemetadata'
