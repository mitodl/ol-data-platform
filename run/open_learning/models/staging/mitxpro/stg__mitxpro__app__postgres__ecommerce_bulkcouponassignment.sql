create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_bulkcouponassignment__dbt_tmp

as (
    with source as (

        select *
        from
            ol_data_lake_production.ol_warehouse_production_raw.raw__xpro__app__postgres__ecommerce_bulkcouponassignment

    )

    , renamed as (

        select
            assignment_sheet_id as bulkcouponassignment_assignment_sheet_id
            , id as bulkcouponassignment_id
            ,
            to_iso8601(from_iso8601_timestamp(assignments_started_date))
            as bulkcouponassignment_assignments_started_on
            ,
            to_iso8601(from_iso8601_timestamp(last_assignment_date))
            as bulkcouponassignment_last_assignment_on
            ,
            to_iso8601(from_iso8601_timestamp(message_delivery_completed_date))

            as bulkcouponassignment_message_delivery_completed_on
            ,
            to_iso8601(from_iso8601_timestamp(sheet_last_modified_date))
            as bulkcouponassignment_sheet_last_modified_on
            ,
            to_iso8601(from_iso8601_timestamp(updated_on))
            as bulkcouponassignment_updated_on
            ,
            to_iso8601(from_iso8601_timestamp(created_on))
            as bulkcouponassignment_created_on

        from source

    )

    select * from renamed
);
