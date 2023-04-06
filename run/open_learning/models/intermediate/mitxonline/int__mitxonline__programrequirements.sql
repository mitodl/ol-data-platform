create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__programrequirements__dbt_tmp

as (
    -- Program Requirement for MITx Online


    with programrequirement as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__courses_programrequirement
    )

    select
        programrequirement_id
        , course_id
        , program_id
        , programrequirement_path
        , programrequirement_depth
        , programrequirement_node_type
        , programrequirement_numchild
        , programrequirement_title
        , programrequirement_operator
        , programrequirement_operator_value
    from programrequirement
);
