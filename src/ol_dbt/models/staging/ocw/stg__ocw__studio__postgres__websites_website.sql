with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__ocw__studio__postgres__websites_website') }}

)

, renamed as (

    select
        uuid as website_uuid
        , name as website_name
        , title as website_title
        , short_id as website_short_id
        , owner_id as website_owner_user_id
        , starter_id as websitestarter_id
        , source as website_source
        , metadata as website_metadata
        , unpublish_status as website_unpublish_status
        , live_publish_status as website_publish_status
        , live_last_published_by_id as website_last_published_by_user_id
        , last_unpublished_by_id as website_last_unpublished_by_user_id
        , url_path as website_url_path
        , if(publish_date is not null and unpublish_status is null, true, false) as website_is_live
        , if(publish_date is not null and unpublish_status = 'succeeded', true, false) as website_is_unpublished
        , if(publish_date is null, true, false) as website_has_never_published
        , if(
            publish_date is not null and unpublish_status is null
            , concat('{{ var("ocw_production_url") }}', url_path), null
        ) as website_live_url
        ,{{ cast_timestamp_to_iso8601('first_published_to_production') }} as website_first_published_on
        ,{{ cast_timestamp_to_iso8601('publish_date') }} as website_publish_date_updated_on
        ,{{ cast_timestamp_to_iso8601('created_on') }} as website_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as website_updated_on
        , nullif(json_query(metadata, 'lax $.primary_course_number' omit quotes), '') as primary_course_number
        , nullif(json_query(metadata, 'lax $.term' omit quotes), '') as metadata_course_term
        , nullif(json_query(metadata, 'lax $.course_title' omit quotes), '') as metadata_course_title
        , nullif(json_query(metadata, 'lax $.year' omit quotes), '') as metadata_course_year
    from source

)

select * from renamed
--exclude test course from the data
where website_name != 'ocw-ci-test-course'
