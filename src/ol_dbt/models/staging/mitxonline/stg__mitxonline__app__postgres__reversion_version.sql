with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__reversion_version') }}

)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at', partition_columns='id') }}

, renamed as (

    select
        id as version_id
        , cast(object_id as integer) as version_object_id
        , revision_id
        , content_type_id as contenttype_id
        , serialized_data as version_object_serialized_data
    from most_recent_source

)

select * from renamed
