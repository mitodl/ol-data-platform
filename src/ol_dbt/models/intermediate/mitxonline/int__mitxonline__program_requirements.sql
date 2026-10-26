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

, course_node_requirements as (
    select
        operator_nodes.programrequirement_id as programrequirement_requirement_id
        , course_nodes.course_id
        , course_nodes.program_id
        , operator_nodes.programrequirement_title
        , operator_nodes.programrequirement_path
        -- course_node_path is the Treebeard path of the course node itself (e.g. 000100010002).
        -- Sorting course nodes within a program by this path yields the canonical display order
        -- defined in the MITx Online CMS, used downstream to populate course_order_in_program.
        , course_nodes.programrequirement_path as course_node_path
        , operator_nodes.programrequirement_operator_value
            as electiveset_required_number
        , case operator_nodes.programrequirement_operator
            when 'min_number_of' then 'Elective'
            when 'all_of' then 'Core'
        end
            as programrequirement_type
        -- A course node is required only when every operator between it and the
        -- program root is all_of. A course under an all_of nested in a "choose N of M"
        -- set is one option in that set, though its innermost operator reads Core.
        -- Partitioned by the course node, as one course can sit under two groups of a program.
        , min(
            case operator_nodes.programrequirement_operator
                when 'all_of' then 1
                when 'min_number_of' then 0
            end
        ) over (partition by course_nodes.programrequirement_id)
            as course_node_is_required

        -- The operators above one course node, innermost first. They sit on one path, so
        -- their lengths differ and the order has no ties.
        , row_number()
            over (
                partition by course_nodes.programrequirement_id
                order by length(operator_nodes.programrequirement_path) desc
            )
            as requirement_nesting
        , max(length(operator_nodes.programrequirement_path))
            over (partition by course_nodes.programrequirement_id)
            as innermost_operator_path_length
    from course_nodes
    left join operator_nodes
        on course_nodes.programrequirement_path like {{ dbt.concat(["operator_nodes.programrequirement_path", "'%'"]) }}
)

-- The program requires a course when any of its course nodes is required.
, all_requirements as (
    select
        course_node_requirements.*
        , max(course_node_requirements.course_node_is_required)
            over (partition by course_node_requirements.course_id, course_node_requirements.program_id) = 1
            as programrequirement_is_required
        -- A course can sit under two groups of a program, e.g. in two tracks. Its row
        -- takes one course node, so its requirement and that requirement's parent come from
        -- the same branch: a node that requires the course first, so the type and nesting
        -- agree with programrequirement_is_required, then the deepest innermost operator,
        -- then the first in tree order.
        , dense_rank()
            over (
                partition by course_node_requirements.course_id, course_node_requirements.program_id
                order by
                    coalesce(course_node_requirements.course_node_is_required, 0) desc
                    , course_node_requirements.innermost_operator_path_length desc
                    , course_node_requirements.course_node_path asc
            )
            as course_node_rank
        -- The display order follows the course's first node in the tree, whichever node
        -- the row takes its requirement from.
        , min(course_node_requirements.course_node_path)
            over (partition by course_node_requirements.course_id, course_node_requirements.program_id)
            as course_first_node_path
    from course_node_requirements
)

, child_requirements as (
    select
        all_requirements.programrequirement_requirement_id
        , all_requirements.course_id
        , all_requirements.program_id
        , all_requirements.programrequirement_type
        , all_requirements.programrequirement_is_required
        , all_requirements.programrequirement_title
        , all_requirements.electiveset_required_number
        , all_requirements.programrequirement_path
        , all_requirements.course_first_node_path
    from all_requirements
    where all_requirements.requirement_nesting = 1 and all_requirements.course_node_rank = 1
)

, parent_requirements as (
    select
        all_requirements.programrequirement_requirement_id
        , all_requirements.course_id
        , all_requirements.program_id
        , all_requirements.programrequirement_type
        , all_requirements.programrequirement_title
        , all_requirements.electiveset_required_number
        , all_requirements.programrequirement_path
    from all_requirements
    where all_requirements.requirement_nesting = 2 and all_requirements.course_node_rank = 1
)

, combined_requirements as (
    select
        child_requirements.programrequirement_requirement_id
        , child_requirements.course_id
        , child_requirements.program_id
        , child_requirements.programrequirement_type
        , child_requirements.programrequirement_is_required
        , child_requirements.programrequirement_title
        , child_requirements.electiveset_required_number
        , child_requirements.course_first_node_path
        , parent_requirements.programrequirement_requirement_id as programrequirement_parent_requirement_id
        , parent_requirements.programrequirement_requirement_id is not null
            as programrequirement_is_a_nested_requirement
    from child_requirements
    left join parent_requirements
        on
            child_requirements.course_id = parent_requirements.course_id
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
    ) as elective_sets
    group by program_id

)

select
    combined_requirements.programrequirement_requirement_id
    , combined_requirements.course_id
    , combined_requirements.program_id
    , combined_requirements.programrequirement_type
    , combined_requirements.programrequirement_is_required
    , combined_requirements.programrequirement_title
    , combined_requirements.electiveset_required_number
    , combined_requirements.programrequirement_parent_requirement_id
    , combined_requirements.programrequirement_is_a_nested_requirement
    , coalesce(core_courses_count.program_num_core_courses, 0)
    + coalesce(elective_courses_count.program_num_elective_courses, 0) as program_num_required_courses
    , row_number() over (
        partition by combined_requirements.program_id
        order by combined_requirements.course_first_node_path
    ) as course_order_in_program
from combined_requirements
left join core_courses_count on combined_requirements.program_id = core_courses_count.program_id
left join elective_courses_count on combined_requirements.program_id = elective_courses_count.program_id
