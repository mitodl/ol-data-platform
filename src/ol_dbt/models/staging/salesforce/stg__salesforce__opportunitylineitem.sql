--- source table contains duplicated rows per primary key. This means there could be multiple copies of the same record,
--- thus deduplicate records here using _airbyte_emitted_at

with source as (

    select *
    from {{ source('ol_warehouse_raw_data', 'raw__thirdparty__salesforce__opportunitylineitem') }}
    where isdeleted = false

)

, source_sorted as (
    select
        *
        , row_number() over (
            partition by id order by _airbyte_emitted_at desc
        ) as row_num
    from source
)

, most_recent_source as (
    select *
    from source_sorted
    where row_num = 1
)

, renamed as (

    select
        id as opportunitylineitem_id
        , opportunityid as opportunity_id
        , name as opportunitylineitem_product_name
        , description as opportunitylineitem_description
        , productcode as opportunitylineitem_product_code
        , listprice as opportunitylineitem_list_price
        , unitprice as opportunitylineitem_sales_price
        , discount as opportunitylineitem_discount_percent
        , quantity as opportunitylineitem_quantity
        , totalprice as opportunitylineitem_total_price
        , {{ cast_date_to_iso8601('servicedate') }} as opportunitylineitem_service_date
        , {{ cast_timestamp_to_iso8601('createddate') }} as opportunitylineitem_created_on
        , {{ cast_timestamp_to_iso8601('systemmodstamp') }} as opportunitylineitem_modified_on
    from most_recent_source
)

select * from renamed
