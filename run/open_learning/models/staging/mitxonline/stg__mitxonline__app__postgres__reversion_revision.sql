create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__reversion_revision__dbt_tmp

as (
    with source as (

        select *
        from ol_data_lake_production.ol_warehouse_production_raw.raw__mitxonline__app__postgres__reversion_revision

    )

    , renamed as (

        select
            id as revision_id
            , comment as revision_comment
            , user_id
            ,
            to_iso8601(from_iso8601_timestamp(date_created))
            as revision_date_created

        from source

    )

    select * from renamed
);
