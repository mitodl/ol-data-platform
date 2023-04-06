

    create table ol_data_lake_qa.ol_warehouse_qa_staging.stg__bootcamps__app__postgres__ecommerce_receipt__dbt_tmp
    WITH (format = 'PARQUET')
  as (
    with source as (

    select * from ol_data_lake_qa.ol_warehouse_qa_raw.raw__bootcamps__app__postgres__ecommerce_receipt

)

, renamed as (
    select
        id as receipt_id
        , order_id
        , data as receipt_data
        ,
    to_iso8601(from_iso8601_timestamp(created_on))
 as receipt_created_on
        ,
    to_iso8601(from_iso8601_timestamp(updated_on))
 as receipt_updated_on
    from source

)

select * from renamed
  );
