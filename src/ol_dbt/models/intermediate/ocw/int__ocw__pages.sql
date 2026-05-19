with websites as (
    select * from {{ ref('stg__ocw__studio__postgres__websites_website') }}
)

, websitecontents as (
    select * from {{ ref('stg__ocw__studio__postgres__websites_websitecontent') }}
)

, websitestarters as (
    select * from {{ ref('stg__ocw__studio__postgres__websites_websitestarter') }}
)

, sitemetadata as (
    select
        website_uuid
        , course_primary_course_number as sitemetadata_primary_course_number
        , course_term as sitemetadata_course_term
        , course_title as sitemetadata_course_title
        , course_year as sitemetadata_course_year
    from websitecontents
    where websitecontent_type = 'sitemetadata'
)

select
    websites.website_name as course_name
    , websites.website_live_url as course_live_url
    , websites.website_uuid as course_uuid
    , websitecontents.websitecontent_type as content_type
    , websitecontents.websitecontent_text_id as page_uuid
    , websitecontents.websitecontent_title as page_title
    , websitecontents.websitecontent_filename as page_filename
    , websitecontents.websitecontent_dirpath as page_dirpath
    , websitecontents.websitecontent_parent_id as page_parent_id
    , websitecontents.websitecontent_is_page as page_is_page_content
    , websitecontents.metadata_draft as page_draft
    , websitecontents.websitecontent_markdown as page_body
    , websitecontents.metadata_description as page_description
    , websitecontents.metadata_legacy_type as page_legacy_type
    , websitecontents.learning_resource_types
    , websitecontents.websitecontent_metadata as metadata --noqa: disable=RF04
    , websites.website_title
    , coalesce(sitemetadata.sitemetadata_primary_course_number, websites.primary_course_number) as course_number
    , coalesce(sitemetadata.sitemetadata_course_term, websites.metadata_course_term) as course_term
    , coalesce(sitemetadata.sitemetadata_course_title, websites.metadata_course_title) as course_title
    , coalesce(sitemetadata.sitemetadata_course_year, websites.metadata_course_year) as course_year
    , websitecontents.websitecontent_created_on as page_created_on
    , websitecontents.websitecontent_updated_on as page_updated_on
    , websites.website_live_url || '/pages/' || websitecontents.websitecontent_filename as page_live_url
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
left join sitemetadata
    on websites.website_uuid = sitemetadata.website_uuid
inner join websitestarters
    on websites.websitestarter_id = websitestarters.websitestarter_id
where
    websitecontents.websitecontent_type = 'page'
    and websitestarters.websitestarter_name = 'ocw-course'
