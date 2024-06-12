with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__ocw__studio__postgres__websites_websitecontent') }}

)

, renamed as (

    select
        id as websitecontent_id
        , website_id as website_uuid
        , title as websitecontent_title
        , text_id as websitecontent_text_id
        , type as websitecontent_type
        , is_page_content as websitecontent_is_page
        , filename as websitecontent_filename
        , dirpath as websitecontent_dirpath
        , file as websitecontent_file
        , parent_id as websitecontent_parent_id
        , owner_id as websitecontent_owner_user_id
        , updated_by_id as websitecontent_updated_by_user_id
        , metadata as websitecontent_metadata
        -- extract website metadata from sitemetadata for courses
        , json_query(metadata, 'lax $.course_description' omit quotes) as course_description
        , nullif(json_query(metadata, 'lax $.term' omit quotes), '') as course_term
        , nullif(json_query(metadata, 'lax $.year' omit quotes), '') as course_year
        -- convert to comma-separated list to be consistent with extra_course_numbers
        , array_join(
            cast(json_parse(json_query(metadata, 'lax $.level')) as array (varchar)), ', ' --noqa
        ) as course_level
        , array_join(
            cast(json_parse(json_query(metadata, 'lax $.learning_resource_types')) as array (varchar)), ', ' --noqa
        ) as course_learning_resource_types
        , array_join(
            cast(json_parse(json_query(metadata, 'lax $.department_numbers')) as array (varchar)), ', ' --noqa
        ) as course_department_numbers
        , json_query(metadata, 'lax $.primary_course_number' omit quotes) as course_primary_course_number
        , json_query(metadata, 'lax $.extra_course_numbers' omit quotes) as course_extra_course_numbers
        , json_query(metadata, 'lax $.instructors.content') as course_instructor_uuids
        , json_query(metadata, 'lax $.topics') as course_topics
        , json_query(metadata, 'lax $.department_numbers') as course_department_numbers_json
        , {{ cast_timestamp_to_iso8601('deleted') }} as websitecontent_deleted_on
        , {{ cast_timestamp_to_iso8601('created_on') }} as websitecontent_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as websitecontent_updated_on
        -- miscellaneous metadata
        , json_query(metadata, 'lax $.body' omit quotes) as metadata_body
        , json_query(metadata, 'lax $.description' omit quotes) as metadata_description
        , json_query(metadata, 'lax $.draft' omit quotes) as metadata_draft
        , json_query(metadata, 'lax $.file' omit quotes) as metadata_file
        , json_query(metadata, 'lax $.file_size' omit quotes) as metadata_file_size
        , json_query(metadata, 'lax $.license' omit quotes) as metadata_license
        , json_query(metadata, 'lax $.ocw_type' omit quotes) as metadata_legacy_type
        , json_query(metadata, 'lax $.resourcetype' omit quotes) as metadata_resource_type
        , json_query(metadata, 'lax $.title' omit quotes) as metadata_title
        , json_query(metadata, 'lax $.uid' omit quotes) as metadata_uid
        -- image_metadata for image resources; could be in metadata or image_metadata
        , json_query(metadata, 'lax $.metadata.image-alt' omit quotes) as metadata_image_alt_text
        , json_query(metadata, 'lax $.metadata.caption' omit quotes) as metadata_image_caption
        , json_query(metadata, 'lax $.metadata.credit' omit quotes) as metadata_image_credit
        , json_query(metadata, 'lax $.image_metadata.image-alt' omit quotes) as image_alt_text
        , json_query(metadata, 'lax $.image_metadata.caption' omit quotes) as image_caption
        , json_query(metadata, 'lax $.image_metadata.credit' omit quotes) as image_credit
        -- video_metadata for video resources
        , json_query(metadata, 'lax $.video_metadata.video_speakers' omit quotes) as video_youtube_speakers
        , json_query(metadata, 'lax $.video_metadata.video_tags' omit quotes) as video_youtube_tags
        , json_query(metadata, 'lax $.video_metadata.youtube_description' omit quotes) as video_youtube_description
        , json_query(metadata, 'lax $.video_metadata.youtube_id' omit quotes) as video_youtube_id
        -- video_files for video resources
        , json_query(metadata, 'lax $.video_files.archive_url' omit quotes) as video_archive_url
        , json_query(metadata, 'lax $.video_files.video_captions_file' omit quotes) as video_captions_file
        , json_query(metadata, 'lax $.video_files.video_thumbnail_file' omit quotes) as video_thumbnail_file
        , json_query(metadata, 'lax $.video_files.video_transcript_file' omit quotes) as video_transcript_file
        -- external resources
        , json_query(metadata, 'lax $.backup_url' omit quotes) as external_resource_backup_url
        , json_query(metadata, 'lax $.external_url' omit quotes) as external_resource_url
        , json_query(metadata, 'lax $.has_external_license_warning' omit quotes) as external_resource_license_warning
        , json_query(metadata, 'lax $.is_broken' omit quotes) as external_resource_broken

    from source

)

select * from renamed
