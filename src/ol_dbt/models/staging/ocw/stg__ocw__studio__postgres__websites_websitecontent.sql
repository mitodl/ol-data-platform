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
        , json_query(metadata, 'lax $.term' omit quotes) as course_term
        , json_query(metadata, 'lax $.year' omit quotes) as course_year
        , json_query(metadata, 'lax $.level' omit quotes) as course_level
        , json_query(metadata, 'lax $.primary_course_number' omit quotes) as course_primary_course_number
        , json_query(metadata, 'lax $.extra_course_numbers' omit quotes) as course_extra_course_numbers
        , json_query(metadata, 'lax $.instructors.content') as course_instructor_uuids
        , json_query(metadata, 'lax $.topics') as course_topics
        , json_query(metadata, 'lax $.learning_resource_types') as course_learning_resource_types
        , json_query(metadata, 'lax $.department_numbers') as course_department_numbers
        , {{ cast_timestamp_to_iso8601('deleted') }} as websitecontent_deleted_on
        , {{ cast_timestamp_to_iso8601('created_on') }} as websitecontent_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as websitecontent_updated_on

    from source

)

select * from renamed
