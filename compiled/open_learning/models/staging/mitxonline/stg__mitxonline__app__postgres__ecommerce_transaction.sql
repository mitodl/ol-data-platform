with source as (

    select * from dev.main_raw.raw__mitxonline__app__postgres__ecommerce_transaction

)

, renamed as (

    select
        id as transaction_id
        , data as transaction_data
        , amount as transaction_amount
        , order_id
        , transaction_id as transaction_readable_identifier
        , transaction_type as transaction_type
        ,
        to_iso8601(from_iso8601_timestamp(created_on))
        as transaction_created_on
        ,
        to_iso8601(from_iso8601_timestamp(updated_on))
        as transaction_updated_on
    from source

)

select * from renamed
