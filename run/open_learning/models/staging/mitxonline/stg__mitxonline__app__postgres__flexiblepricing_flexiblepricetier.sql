create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__flexiblepricing_flexiblepricetier__dbt_tmp

as (
    with source as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_raw.raw__mitxonline__app__postgres__flexiblepricing_flexiblepricetier
    )

    , renamed as (
        select
            source.id as flexiblepricetier_id
            , source."current" as flexiblepricetier_is_current
            , source.discount_id as discount_id
            , source.courseware_object_id
            , source.income_threshold_usd as flexiblepricetier_income_threshold_usd
            , source.courseware_content_type_id as contenttype_id
            ,
            to_iso8601(from_iso8601_timestamp(source.created_on))
            as flexiblepricetier_created_on
            ,
            to_iso8601(from_iso8601_timestamp(source.updated_on))
            as flexiblepricetier_updated_on

        from source
    )

    select * from renamed
);
