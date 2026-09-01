with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__reversion_revision') }}

)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at', partition_columns='id') }}

, renamed as (

    select
        id as revision_id
        , comment as revision_comment
        , user_id
        ,{{ cast_timestamp_to_iso8601('date_created') }} as revision_date_created

    from most_recent_source

)

select * from renamed
