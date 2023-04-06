

    create table ol_data_lake_qa.ol_warehouse_qa_staging.stg__bootcamps__app__postgres__ecommerce_wiretransferreceipt__dbt_tmp
    WITH (format = 'PARQUET')
  as (
    with source as (

    select * from ol_data_lake_qa.ol_warehouse_qa_raw.raw__bootcamps__app__postgres__ecommerce_wiretransferreceipt

)

, renamed as (
    select
        id as wiretransferreceipt_id
        , order_id
        , data as wiretransferreceipt_data
        ,
    to_iso8601(from_iso8601_timestamp(created_on))
 as wiretransferreceipt_created_on
        ,
    to_iso8601(from_iso8601_timestamp(updated_on))
 as wiretransferreceipt_updated_on
        , wire_transfer_id as wiretransferreceipt_import_id
    from source

)

select * from renamed
  );
