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
    , websites.website_short_id as course_readable_id
    , websites.website_source as course_source
    , websites.website_is_live as course_is_live
    , websites.website_is_unpublished as course_is_unpublished
    , websites.website_has_never_published as course_has_never_published
    , websites.website_url_path as course_url_path
    , websites.website_live_url as course_live_url
    , websites.website_first_published_on as course_first_published_on
    , websites.website_publish_date_updated_on as course_publish_date_updated_on
    , websites.website_created_on as course_created_on
    , websites.website_updated_on as course_updated_on
    , websitecontents.course_description
    , websitecontents.course_term
    , websitecontents.course_year
    , websitecontents.course_level
    , websitecontents.course_primary_course_number
    , websitecontents.course_extra_course_numbers
    , websitecontents.course_topics
    , websitecontents.course_learning_resource_types
from websites
inner join websitecontents
    on websites.website_uuid = websitecontents.website_uuid
inner join websitestarters
    on websites.websitestarter_id = websitestarters.websitestarter_id
--- the where clause ensure the records are OCW courses
where
    websitecontents.websitecontent_type = 'sitemetadata'
    and websitestarters.websitestarter_name = 'ocw-course'
