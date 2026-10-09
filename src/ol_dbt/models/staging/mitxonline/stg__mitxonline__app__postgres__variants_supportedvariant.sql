with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__variants_supportedvariant') }}
)

{{ deduplicate_raw_table(raw_table='raw__mitxonline__app__postgres__variants_supportedvariant', partition_columns='id') }}

, cleaned as (
    select
        id as supportedvariant_id
        , content_type_id as contenttype_id
        , object_id as supportedvariant_object_id
        , language as supportedvariant_language
        , variant_length as supportedvariant_length
        , variant_industry as supportedvariant_industry
        , active as supportedvariant_is_active
        , b2b_only as supportedvariant_is_b2b_only
        , default_variant as supportedvariant_is_default
        ,{{ cast_timestamp_to_iso8601('created_on') }} as supportedvariant_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as supportedvariant_updated_on
    from most_recent_source
)

select * from cleaned
