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
        , markdown as websitecontent_markdown
        , parent_id as websitecontent_parent_id
        , owner_id as websitecontent_owner_user_id
        , updated_by_id as websitecontent_updated_by_user_id
        , metadata as websitecontent_metadata
        -- extract website metadata from sitemetadata for courses
        , {{ json_query_string('metadata', "'$.course_description'") }} as course_description
        , nullif({{ json_query_string('metadata', "'$.course_title'") }}, '') as course_title
        , nullif({{ json_query_string('metadata', "'$.term'") }}, '') as course_term
        , nullif({{ json_query_string('metadata', "'$.year'") }}, '') as course_year
        -- convert JSON arrays to comma-separated strings using cross-db macros
        , {{ array_join(json_extract_varchar_array('metadata', "'$.level'"), ', ') }} as course_level
        , {{ array_join(json_extract_varchar_array('metadata', "'$.learning_resource_types'"), ', ') }} as learning_resource_types
        , {{ array_join(json_extract_varchar_array('metadata', "'$.department_numbers'"), ', ') }} as course_department_numbers
        , nullif({{ json_query_string('metadata', "'$.primary_course_number'") }}, '') as course_primary_course_number
        , {{ json_query_string('metadata', "'$.extra_course_numbers'") }} as course_extra_course_numbers
        , {{ json_extract_value('metadata', "'$.instructors.content'") }} as course_instructor_uuids
        , {{ json_extract_value('metadata', "'$.topics'") }} as course_topics
        , {{ json_extract_value('metadata', "'$.department_numbers'") }} as course_department_numbers_json
        -- course_image is an object {content: <text_id>, website: <short_id>}; extract the text_id
        -- to join against websitecontent for the image file path
        , nullif({{ json_query_string('metadata', "'$.course_image.content'") }}, '') as course_image_text_id
        , {{ cast_timestamp_to_iso8601('deleted') }} as websitecontent_deleted_on
        , {{ cast_timestamp_to_iso8601('created_on') }} as websitecontent_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as websitecontent_updated_on
        -- miscellaneous metadata
        , {{ json_query_string('metadata', "'$.body'") }} as metadata_body
        , {{ json_query_string('metadata', "'$.description'") }} as metadata_description
        , cast(nullif({{ json_query_string('metadata', "'$.draft'") }}, '') as boolean) as metadata_draft
        , {{ json_query_string('metadata', "'$.file'") }} as metadata_file
        , {{ json_query_string('metadata', "'$.file_size'") }} as metadata_file_size
        , {{ json_query_string('metadata', "'$.license'") }} as metadata_license
        , {{ json_query_string('metadata', "'$.ocw_type'") }} as metadata_legacy_type
        , {{ json_query_string('metadata', "'$.resourcetype'") }} as metadata_resource_type
        , {{ json_query_string('metadata', "'$.title'") }} as metadata_title
        , {{ json_query_string('metadata', "'$.uid'") }} as metadata_uid

    from source

)

select * from renamed
