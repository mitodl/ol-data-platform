with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__ecommerce_linerunselection') }}

)

, renamed as (

    select
        id as linerunselection_id
        , line_id
        , run_id as courserun_id
        , to_iso8601(from_iso8601_timestamp(created_on)) as linerunselection_created_on
        , to_iso8601(from_iso8601_timestamp(updated_on)) as linerunselection_updated_on
    from source

)

select * from renamed
