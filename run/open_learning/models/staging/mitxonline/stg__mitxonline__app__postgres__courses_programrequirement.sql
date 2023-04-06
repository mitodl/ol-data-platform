create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__courses_programrequirement__dbt_tmp

as (
    -- MITx Online Program Requirement Information

    with source as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_raw.raw__mitxonline__app__postgres__courses_programrequirement
    )

    , cleaned as (
        select
            id as programrequirement_id
            , course_id
            , program_id
            , path as programrequirement_path
            , depth as programrequirement_depth
            , node_type as programrequirement_node_type
            , numchild as programrequirement_numchild
            , title as programrequirement_title
            , operator as programrequirement_operator
            , operator_value as programrequirement_operator_value
        from source
    )

    select * from cleaned
);
