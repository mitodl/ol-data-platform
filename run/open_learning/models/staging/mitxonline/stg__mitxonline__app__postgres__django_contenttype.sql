create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__django_contenttype__dbt_tmp

as (
    with source as (

        select *
        from ol_data_lake_production.ol_warehouse_production_raw.raw__mitxonline__app__postgres__django_content_type

    )

    , renamed as (

        select
            id as contenttype_id
            , concat_ws(
                '_'
                , app_label
                , model
            ) as contenttype_full_name
        from source

    )

    select * from renamed
);
