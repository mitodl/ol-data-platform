with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__reversion_revision') }}

)

, renamed as (

    select
        id as revision_id
        , comment as revision_comment
        , user_id
        , {{ cast_timestamp_to_iso8601('date_created') }} as revision_date_created

    from source

)

select * from renamed
