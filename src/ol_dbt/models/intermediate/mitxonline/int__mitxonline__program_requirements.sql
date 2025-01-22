-- Program Requirement for MITx Online
with program_requirements as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__courses_programrequirement') }}
)

, operator_nodes as (
    select *
    from program_requirements
    where programrequirement_node_type = 'operator'
)

, course_nodes as (
    select *
    from program_requirements
    where programrequirement_node_type = 'course'
)

, all_requirements as (
    select
        operator_nodes.programrequirement_id as programrequirement_requirement_id
        , course_nodes.course_id
        , course_nodes.program_id
        , operator_nodes.programrequirement_title
        , operator_nodes.programrequirement_path
        , operator_nodes.programrequirement_operator_value
        as electiveset_required_number
        , case operator_nodes.programrequirement_operator
            when 'min_number_of' then 'Elective'
            when 'all_of' then 'Core'
        end
        as programrequirement_type

        , row_number()
            over (
                partition by course_nodes.course_id, course_nodes.program_id
                order by length(operator_nodes.programrequirement_path) desc
            )
        as requirement_nesting
    from course_nodes
    left join operator_nodes
        on course_nodes.programrequirement_path like operator_nodes.programrequirement_path || '%'
)

, child_requirements as (
    select
        programrequirement_requirement_id
        , course_id
        , program_id
        , programrequirement_type
        , programrequirement_title
        , electiveset_required_number
        , programrequirement_path
    from all_requirements
    where all_requirements.requirement_nesting = 1
)

, parent_requirements as (
    select
        programrequirement_requirement_id
        , course_id
        , program_id
        , programrequirement_type
        , programrequirement_title
        , electiveset_required_number
        , programrequirement_path
    from all_requirements
    where all_requirements.requirement_nesting = 2
)

, combined_requirements as (
    select
        child_requirements.programrequirement_requirement_id
        , child_requirements.course_id
        , child_requirements.program_id
        , child_requirements.programrequirement_type
        , child_requirements.programrequirement_title
        , child_requirements.electiveset_required_number
        , parent_requirements.programrequirement_requirement_id as programrequirement_parent_requirement_id
        , parent_requirements.programrequirement_requirement_id is not null
        as programrequirement_is_a_nested_requirement
    from child_requirements
    left join parent_requirements
        on
            child_requirements.programrequirement_path like parent_requirements.programrequirement_path || '%'
            and child_requirements.course_id = parent_requirements.course_id
            and child_requirements.program_id = parent_requirements.program_id
)

, core_courses_count as (
    select
        program_id
        , count_if(programrequirement_type = 'Core') as program_num_core_courses
    from combined_requirements
    group by program_id
)

, elective_courses_count as (
    select
        program_id
        , sum(electiveset_required_number) as program_num_elective_courses
    from (
        select
            combined_requirements.program_id
            , combined_requirements.programrequirement_requirement_id
            , avg(combined_requirements.electiveset_required_number) as electiveset_required_number
        from combined_requirements
        where
            combined_requirements.programrequirement_type = 'Elective'
            and combined_requirements.programrequirement_is_a_nested_requirement = false
        group by
            combined_requirements.program_id
            , combined_requirements.programrequirement_requirement_id
    )
    group by program_id

)

select
    combined_requirements.*
    , coalesce(core_courses_count.program_num_core_courses, 0)
    + coalesce(elective_courses_count.program_num_elective_courses, 0) as program_num_required_courses
from combined_requirements
left join core_courses_count on combined_requirements.program_id = core_courses_count.program_id
left join elective_courses_count on combined_requirements.program_id = elective_courses_count.program_id
