with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__reversion_version') }}

)

, renamed as (

    select
        id as version_id
        , cast(object_id as integer) as version_object_id
        , revision_id
        , content_type_id as contenttype_id
        , serialized_data as version_object_serialized_data
    from source

)

select * from renamed
