with websites as (
    select * from {{ ref('stg__ocw__studio__postgres__websites_website') }}
)

, websitecontents as (
    select * from {{ ref('stg__ocw__studio__postgres__websites_websitecontent') }}
)

, websitestarters as (
    select * from {{ ref('stg__ocw__studio__postgres__websites_websitestarter') }}
)

select
    websites.website_uuid as course_uuid
    , websites.website_name as course_name
    , websites.website_title as course_title
    , websitecontents.metadata_resource_type as resource_type
    , websitecontents.websitecontent_text_id as resource_uuid
    , websitecontents.websitecontent_filename as resource_filename
    , websitecontents.metadata_draft as resource_draft
    , websites.primary_course_number as course_number
    , 'https://ocw-studio.odl.mit.edu/sites/'
    || websites.website_name
    || '/type/'
    || websitecontents.websitecontent_type
    || '/edit/'
    || websitecontents.websitecontent_text_id
    || '/' as studio_url

from websites
inner join websitecontents
    on websites.website_uuid = websitecontents.website_uuid
inner join websitestarters
    on websites.websitestarter_id = websitestarters.websitestarter_id
where
    websitecontents.websitecontent_type = 'resource'
    and websitestarters.websitestarter_name = 'ocw-course'
