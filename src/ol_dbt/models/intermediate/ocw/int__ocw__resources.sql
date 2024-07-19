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
    , websitecontents.websitecontent_metadata as metadata --noqa: disable=RF04
    , websitecontents.websitecontent_type as content_type
    , websitecontents.learning_resource_types
    , websitecontents.websitecontent_title as resource_title
    -- image_metadata for image resources; could be in metadata or image_metadata
    -- noqa: disable=RF02
    , cast(
        nullif(json_query(websitecontents.websitecontent_metadata, 'lax $.is_broken' omit quotes), '') as boolean
    ) as external_resource_is_broken
    , cast(
        nullif(
            json_query(websitecontents.websitecontent_metadata, 'lax $.has_external_license_warning' omit quotes), ''
        ) as boolean
    ) as external_resource_license_warning
    , 'https://ocw-studio.odl.mit.edu/sites/'
    || websites.website_name
    || '/type/'
    || websitecontents.websitecontent_type
    || '/edit/'
    || websitecontents.websitecontent_text_id
    || '/' as studio_url
    -- video_metadata for video resources
    , coalesce(
        nullif(json_query(websitecontents.websitecontent_metadata, 'lax $.metadata.image_alt' omit quotes), '')
        , nullif(
            json_query(websitecontents.websitecontent_metadata, 'lax $.image_metadata."image-alt"' omit quotes), ''
        )
    ) as image_alt_text
    , coalesce(
        nullif(json_query(websitecontents.websitecontent_metadata, 'lax $.metadata.caption' omit quotes), '')
        , nullif(json_query(websitecontents.websitecontent_metadata, 'lax $.image_metadata.caption' omit quotes), '')
    ) as image_caption
    , coalesce(
        nullif(json_query(websitecontents.websitecontent_metadata, 'lax $.metadata.credit' omit quotes), '')
        , nullif(json_query(websitecontents.websitecontent_metadata, 'lax $.image_metadata.credit' omit quotes), '')
    ) as image_credit
    , json_query(
        websitecontents.websitecontent_metadata, 'lax $.video_metadata.video_speakers' omit quotes
    ) as video_youtube_speakers
    -- video_files for video resources
    , json_query(
        websitecontents.websitecontent_metadata, 'lax $.video_metadata.video_tags' omit quotes
    ) as video_youtube_tags
    , json_query(
        websitecontents.websitecontent_metadata, 'lax $.video_metadata.youtube_description' omit quotes
    ) as video_youtube_description
    , json_query(
        websitecontents.websitecontent_metadata, 'lax $.video_metadata.youtube_id' omit quotes
    ) as video_youtube_id
    , json_query(
        websitecontents.websitecontent_metadata, 'lax $.video_files.archive_url' omit quotes
    ) as video_archive_url
    -- external resources
    , json_query(
        websitecontents.websitecontent_metadata, 'lax $.video_files.video_captions_file' omit quotes
    ) as video_captions_file
    , json_query(
        websitecontents.websitecontent_metadata, 'lax $.video_files.video_thumbnail_file' omit quotes
    ) as video_thumbnail_file
    , json_query(
        websitecontents.websitecontent_metadata, 'lax $.video_files.video_transcript_file' omit quotes
    ) as video_transcript_file
    , json_query(
        websitecontents.websitecontent_metadata, 'lax $.backup_url' omit quotes
    ) as external_resource_backup_url
    , json_query(websitecontents.websitecontent_metadata, 'lax $.external_url' omit quotes) as external_resource_url
from websites
inner join websitecontents
    on websites.website_uuid = websitecontents.website_uuid
inner join websitestarters
    on websites.websitestarter_id = websitestarters.websitestarter_id
where
    (websitecontents.websitecontent_type = 'resource' or websitecontents.websitecontent_type = 'external-resource')
    and websitestarters.websitestarter_name = 'ocw-course'
