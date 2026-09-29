with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__ecommerce_courserunselection') }}

)

{{ deduplicate_raw_table(raw_table='raw__xpro__app__postgres__ecommerce_courserunselection', partition_columns='id') }}
, renamed as (

    select
        id as basketrunselection_id
        , basket_id
        , run_id as courserun_id
        ,{{ cast_timestamp_to_iso8601('created_on') }} as basketrunselection_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as basketrunselection_updated_on
    from most_recent_source

)

select * from renamed
