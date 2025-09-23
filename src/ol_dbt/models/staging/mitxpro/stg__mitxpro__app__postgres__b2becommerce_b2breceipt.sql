with
    source as (

        select * from {{ source("ol_warehouse_raw_data", "raw__xpro__app__postgres__b2b_ecommerce_b2breceipt") }}

    ),
    renamed as (

        select
            id as b2breceipt_id,
            order_id as b2border_id,
            data as b2breceipt_data,
            json_query(data, 'lax $.decision' omit quotes) as b2breceipt_transaction_status,
            json_query(data, 'lax $.transaction_id' omit quotes) as b2breceipt_transaction_id,
            json_query(data, 'lax $.auth_code' omit quotes) as b2breceipt_authorization_code,
            json_query(data, 'lax $.req_reference_number' omit quotes) as b2breceipt_reference_number,
            json_query(data, 'lax $.req_payment_method' omit quotes) as b2breceipt_payment_method,
            json_query(data, 'lax $.req_bill_to_address_state' omit quotes) as b2breceipt_bill_to_address_state,
            json_query(data, 'lax $.req_bill_to_address_country' omit quotes) as b2breceipt_bill_to_address_country,
            {{ cast_timestamp_to_iso8601("created_on") }} as b2breceipt_created_on,
            {{ cast_timestamp_to_iso8601("updated_on") }} as b2breceipt_updated_on
        from source

    )

select *
from renamed
