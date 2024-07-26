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
        , nullif(
            json_query(websitecontent_metadata, 'lax $.primary_course_number' omit quotes), ''
        ) as sitemetadata_primary_course_number
        , nullif(json_query(websitecontent_metadata, 'lax $.term' omit quotes), '') as sitemetadata_course_term
        , nullif(json_query(websitecontent_metadata, 'lax $.year' omit quotes), '') as sitemetadata_course_year
        , nullif(json_query(websitecontent_metadata, 'lax $.course_title' omit quotes), '') as sitemetadata_course_title
    from websitecontents
    where websitecontent_type = 'sitemetadata'
)

select
    websites.website_name as course_name
    , websites.website_uuid as course_uuid
    , websitecontents.websitecontent_type as content_type
    , websitecontents.learning_resource_types
    , websitecontents.websitecontent_metadata as metadata --noqa: disable=RF04
    , websitecontents.metadata_draft as resource_draft
    , websitecontents.websitecontent_filename as resource_filename
    , websitecontents.websitecontent_title as resource_title
    , websitecontents.metadata_resource_type as resource_type
    , websitecontents.websitecontent_text_id as resource_uuid
    , websites.website_title
    -- noqa: disable=RF02
    -- external resources
    , cast(
        nullif(json_query(websitecontents.websitecontent_metadata, 'lax $.is_broken' omit quotes), '') as boolean
    ) as external_resource_is_broken
    , cast(
        nullif(
            json_query(websitecontents.websitecontent_metadata, 'lax $.has_external_license_warning' omit quotes), ''
        ) as boolean
    ) as external_resource_license_warning
    , coalesce(sitemetadata.sitemetadata_primary_course_number, websites.primary_course_number) as course_number
    , coalesce(sitemetadata.sitemetadata_course_term, websites.metadata_course_term) as course_term
    , coalesce(sitemetadata.sitemetadata_course_title, websites.metadata_course_title) as course_title
    , coalesce(sitemetadata.sitemetadata_course_year, websites.metadata_course_year) as course_year
    , 'https://ocw-studio.odl.mit.edu/sites/'
    || websites.website_name
    || '/type/'
    || websitecontents.websitecontent_type
    || '/edit/'
    || websitecontents.websitecontent_text_id
    || '/' as studio_url
    , nullif(json_query(
        websitecontents.websitecontent_metadata, 'lax $.backup_url' omit quotes
    ), '') as external_resource_backup_url
    , json_query(websitecontents.websitecontent_metadata, 'lax $.external_url' omit quotes) as external_resource_url
    -- image_metadata for image resources; could be in metadata or image_metadata
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
    -- video_metadata for video resources
    , nullif(json_query(
        websitecontents.websitecontent_metadata, 'lax $.video_metadata.youtube_description' omit quotes
    ), '') as video_youtube_description
    , nullif(json_query(
        websitecontents.websitecontent_metadata, 'lax $.video_metadata.youtube_id' omit quotes
    ), '') as video_youtube_id
    , nullif(json_query(
        websitecontents.websitecontent_metadata, 'lax $.video_metadata.video_speakers' omit quotes
    ), '') as video_youtube_speakers
    , nullif(json_query(
        websitecontents.websitecontent_metadata, 'lax $.video_metadata.video_tags' omit quotes
    ), '') as video_youtube_tags
    -- video_files for video resources
    , nullif(json_query(
        websitecontents.websitecontent_metadata, 'lax $.video_files.archive_url' omit quotes
    ), '') as video_archive_url
    , nullif(json_query(
        websitecontents.websitecontent_metadata, 'lax $.video_files.video_captions_file' omit quotes
    ), '') as video_captions_file
    , nullif(json_query(
        websitecontents.websitecontent_metadata, 'lax $.video_files.video_thumbnail_file' omit quotes
    ), '') as video_thumbnail_file
    , nullif(json_query(
        websitecontents.websitecontent_metadata, 'lax $.video_files.video_transcript_file' omit quotes
    ), '') as video_transcript_file
from websites
inner join websitecontents
    on websites.website_uuid = websitecontents.website_uuid
left join sitemetadata
    on websites.website_uuid = sitemetadata.website_uuid
inner join websitestarters
    on websites.websitestarter_id = websitestarters.websitestarter_id
where
    (websitecontents.websitecontent_type = 'resource' or websitecontents.websitecontent_type = 'external-resource')
    and websitestarters.websitestarter_name = 'ocw-course'
