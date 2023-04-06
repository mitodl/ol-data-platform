create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__program_to_courses__dbt_tmp

as (
    -- Program to Course information for MITx Online
    -- course and program relationship in this model is many to many
    -- courses are grouped into "required" and "elective" categories from Program Requirement


    with programrequirement as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__courses_programrequirement
    )

    , required_path as (
        select programrequirement_path
        from programrequirement
        where programrequirement_node_type = 'operator' and programrequirement_operator = 'all_of'
    )

    , required_courses as (
        select
            programrequirement.programrequirement_id
            , programrequirement.course_id
            , programrequirement.program_id
        from programrequirement
        inner join required_path
            on programrequirement.programrequirement_path like required_path.programrequirement_path || '%'
        where programrequirement.programrequirement_node_type = 'course'
    )

    --- elective courses could be nested. However, courses that are not required are elective
    , elective_courses as (
        select
            programrequirement.programrequirement_id
            , programrequirement.course_id
            , programrequirement.program_id
        from programrequirement
        left join required_courses
            on
                programrequirement.program_id = required_courses.program_id
                and programrequirement.course_id = required_courses.course_id
        where
            programrequirement.programrequirement_node_type = 'course'
            and required_courses.program_id is null
    )

    , combined as (
        select
            programrequirement_id
            , course_id
            , program_id
            , 'Required Courses' as programrequirement_type
        from required_courses

        union all

        select
            programrequirement_id
            , course_id
            , program_id
            , 'Elective Courses' as programrequirement_type
        from elective_courses
    )

    select * from combined
);
