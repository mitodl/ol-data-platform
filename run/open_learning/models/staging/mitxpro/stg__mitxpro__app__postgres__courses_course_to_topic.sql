create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__courses_course_to_topic__dbt_tmp

as (
    -- MITxPro Course to Topic Information

    with source as (
        select *
        from ol_data_lake_production.ol_warehouse_production_raw.raw__xpro__app__postgres__courses_course_topics
    )

    , cleaned as (
        select
            id as coursetotopic_id
            , course_id
            , coursetopic_id
        from source
    )

    select * from cleaned
);
