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
        -- image_metadata for image resources
        , json_query(metadata, 'lax $.image_metadata') as image_metadata
        , json_query(image_metadata, 'lax $.credit' omit quotes) as image_credit
        , json_query(image_metadata, 'lax $.caption' omit quotes) as image_caption
        , json_query(image_metadata, 'lax $.alt_text' omit quotes) as image_alt_text
        -- video_metadata for video resources
        , json_query(metadata, 'lax $.video_metadata') as video_metadata
        , json_query(video_metadata, 'lax $.youtube_id' omit quotes) as video_youtube_id
        , json_query(video_metadata, 'lax $.youtube_description' omit quotes) as video_youtube_description
        , json_query(video_metadata, 'lax $.video_speakers' omit quotes) as video_youtube_speakers
        , json_query(video_metadata, 'lax $.video_tags' omit quotes) as video_youtube_tags

    from source

)

select * from renamed
