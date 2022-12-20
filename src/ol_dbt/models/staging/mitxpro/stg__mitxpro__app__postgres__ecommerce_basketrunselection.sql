with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__ecommerce_courserunselection') }}

)

, renamed as (

    select
        id as basketrunselection_id
        , basket_id
        , run_id as courserun_id
        , to_iso8601(from_iso8601_timestamp(created_on)) as basketrunselection_created_on
        , to_iso8601(from_iso8601_timestamp(updated_on)) as basketrunselection_updated_on
    from source

)

select * from renamed
