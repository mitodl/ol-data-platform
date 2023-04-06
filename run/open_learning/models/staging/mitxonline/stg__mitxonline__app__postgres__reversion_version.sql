create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__reversion_version__dbt_tmp

as (
    with source as (

        select *
        from ol_data_lake_production.ol_warehouse_production_raw.raw__mitxonline__app__postgres__reversion_version

    )

    , renamed as (

        select
            id as version_id
            , cast(object_id as integer) as version_object_id
            , revision_id
            , content_type_id as contenttype_id
            , serialized_data as version_object_serialized_data
        from source

    )

    select * from renamed
);
