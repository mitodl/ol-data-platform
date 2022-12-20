with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__ecommerce_programrunline') }}

)

, renamed as (

    select
        id as programrunline_id
        , line_id
        , program_run_id as programrun_id
        , to_iso8601(from_iso8601_timestamp(created_on)) as programrunline_created_on
        , to_iso8601(from_iso8601_timestamp(updated_on)) as programrunline_updated_on
    from source

)

select * from renamed
