with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__wagtailimages_image') }}
)

, cleaned as (
    select
        id as image_id
        , file as image_file
        , concat('{{ var("xpro_media_url") }}', file) as image_url
        , title as image_title
        , width as image_width
        , height as image_height
        , file_hash as image_file_hash
        , file_size as image_file_size
        , {{ cast_timestamp_to_iso8601('created_at') }} as image_created_on
        , collection_id
        , focal_point_x as image_focal_point_x
        , focal_point_y as image_focal_point_y
        , focal_point_width as image_focal_point_width
        , focal_point_height as image_focal_point_height
        , uploaded_by_user_id
        , description as image_description
    from source
)

select * from cleaned
