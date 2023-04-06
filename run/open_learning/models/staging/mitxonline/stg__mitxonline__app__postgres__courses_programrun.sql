create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__courses_programrun__dbt_tmp

as (
    with source as (
        select *
        from ol_data_lake_production.ol_warehouse_production_raw.raw__mitxonline__app__postgres__courses_programrun
    )

    , renamed as (

        select
            id as programrun_id
            , run_tag as programrun_tag
            , program_id
            ,
            to_iso8601(from_iso8601_timestamp(start_date))
            as programrun_start_on
            ,
            to_iso8601(from_iso8601_timestamp(end_date))
            as programrun_end_on
            ,
            to_iso8601(from_iso8601_timestamp(updated_on))
            as programrun_updated_on
            ,
            to_iso8601(from_iso8601_timestamp(created_on))
            as programrun_created_on
        from source

    )

    select * from renamed
);
