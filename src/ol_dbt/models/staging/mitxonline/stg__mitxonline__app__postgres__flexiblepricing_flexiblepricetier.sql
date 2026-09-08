with source as (
    select *
    from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__flexiblepricing_flexiblepricetier') }}
)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at', partition_columns='id') }}

, renamed as (
    select
        id as flexiblepricetier_id
        , "current" as flexiblepricetier_is_current
        , discount_id
        , courseware_object_id
        , income_threshold_usd as flexiblepricetier_income_threshold_usd
        , courseware_content_type_id as contenttype_id
        ,{{ cast_timestamp_to_iso8601('created_on') }} as flexiblepricetier_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as flexiblepricetier_updated_on

    from most_recent_source
)

select * from renamed
