with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__b2b_ecommerce_b2breceipt') }}

)

, renamed as (

    select
        id as b2breceipt_id
        , order_id as b2border_id
        , data as b2breceipt_data
        , {{ json_query_string('data', "'$.decision'") }} as b2breceipt_transaction_status
        , {{ json_query_string('data', "'$.transaction_id'") }} as b2breceipt_transaction_id
        , {{ json_query_string('data', "'$.auth_code'") }} as b2breceipt_authorization_code
        , {{ json_query_string('data', "'$.req_reference_number'") }} as b2breceipt_reference_number
        , {{ json_query_string('data', "'$.req_payment_method'") }} as b2breceipt_payment_method
        , {{ json_query_string('data', "'$.req_bill_to_address_state'") }} as b2breceipt_bill_to_address_state
        , {{ json_query_string('data', "'$.req_bill_to_address_country'") }} as b2breceipt_bill_to_address_country
        ,{{ cast_timestamp_to_iso8601('created_on') }} as b2breceipt_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as b2breceipt_updated_on
    from source

)

select * from renamed
