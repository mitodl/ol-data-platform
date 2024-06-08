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

from websites
inner join websitecontents
    on websites.website_uuid = websitecontents.website_uuid
inner join websitestarters
    on websites.websitestarter_id = websitestarters.websitestarter_id
where
    websitecontents.websitecontent_type not in ('navmenu', 'sitemetadata')
    and websitestarters.websitestarter_name = 'ocw-course'
