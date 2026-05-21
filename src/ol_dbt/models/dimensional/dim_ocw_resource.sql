{{ config(
    materialized='table'
) }}

select
    course_uuid
    , course_name
    , course_number
    , course_title
    , course_term
    , course_year
    , course_live_url
    , resource_uuid
    , resource_title
    , content_type
    , resource_type
    , resource_ocw_type
    , resource_filename
    , resource_file_type
    , resource_file_size
    , resource_draft
    , resource_live_url
    , studio_url
    , website_title
    , learning_resource_types
    , resource_license
    , resource_description
    , resource_audience
    , resource_level
    , external_resource_url
    , external_resource_is_broken
    , external_resource_license_warning
    , external_resource_url_status_code
    , external_resource_backup_url
    , external_resource_backup_url_status_code
    , external_resource_status
    , external_resource_wayback_url
    , image_alt_text
    , image_caption
    , image_credit
    , video_youtube_id
    , video_youtube_description
    , video_youtube_speakers
    , video_youtube_tags
    , video_archive_url
    , video_captions_file
    , video_thumbnail_file
    , video_transcript_file
from {{ ref('int__ocw__resources') }}
