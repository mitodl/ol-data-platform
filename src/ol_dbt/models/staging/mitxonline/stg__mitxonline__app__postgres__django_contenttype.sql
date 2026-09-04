with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__django_content_type') }}

)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at', partition_columns='id') }}

, renamed as (

    select
        id as contenttype_id
        , concat_ws(
            '_'
            , app_label
            , model
        ) as contenttype_full_name
    from most_recent_source

)

select * from renamed
