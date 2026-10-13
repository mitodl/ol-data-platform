{#
  integrations__learn__mitxonline_program_children
  The courses and child programs of each program in integrations__learn__mitxonline_programs,
  in the order MIT Learn's MITx Online ETL takes from the program's requirement tree
  (get_child_positions and get_program_ids_from_req_tree in learning_resources/etl/mitxonline.py).
  Contract: docs/learn_marts_contract.md
#}

with programs as (
    select
        program_id
        , program_readable_id
        , program_display_mode
    from {{ ref('stg__mitxonline__app__postgres__courses_program') }}
    where program_is_live and not coalesce(program_is_b2b_only, false)
)

-- treebeard orders a tree by path, so path order is the order the API nests the nodes in
, nodes as (
    select
        program_id
        , course_id
        , required_program_id
        , programrequirement_node_type
        , programrequirement_path
    from {{ ref('stg__mitxonline__app__postgres__courses_programrequirement') }}
    where programrequirement_node_type in ('course', 'program')
)

-- The first node of a program for each program it requires: the ETL follows a required
-- program once. A required program the API does not list is skipped.
, program_nodes as (
    select
        nodes.program_id
        , nodes.required_program_id
        , nodes.programrequirement_path
        , required_programs.program_readable_id as required_program_readable_id
        , required_programs.program_display_mode as required_program_display_mode
        , row_number() over (
            partition by nodes.program_id, nodes.required_program_id
            order by nodes.programrequirement_path
        ) as node_rank
    from nodes
    inner join programs as required_programs on nodes.required_program_id = required_programs.program_id
    where nodes.programrequirement_node_type = 'program'
)

, entries as (
    select
        nodes.program_id
        , 'course' as child_resource_type
        , nodes.course_id as child_id
        , nodes.programrequirement_path as node_path
        , '' as nested_path
    from nodes
    where nodes.programrequirement_node_type = 'course' and nodes.course_id is not null
    union all
    -- A required program shown as a course is a child in its own right
    select
        program_nodes.program_id
        , 'program' as child_resource_type
        , program_nodes.required_program_id as child_id
        , program_nodes.programrequirement_path as node_path
        , '' as nested_path
    from program_nodes
    where program_nodes.node_rank = 1
    union all
    -- Any other required program stands for its own courses, placed where its node is.
    -- Programs it requires in turn are not followed: no required program has any today.
    select
        program_nodes.program_id
        , 'course' as child_resource_type
        , nested.course_id as child_id
        , program_nodes.programrequirement_path as node_path
        , nested.programrequirement_path as nested_path
    from program_nodes
    inner join nodes as nested on program_nodes.required_program_id = nested.program_id
    where
        program_nodes.node_rank = 1
        and coalesce(program_nodes.required_program_display_mode, '') != 'course'
        and nested.programrequirement_node_type = 'course'
        and nested.course_id is not null
)

, first_entries as (
    select
        *
        , row_number() over (
            partition by program_id, child_resource_type, child_id
            order by node_path, nested_path
        ) as entry_rank
    from entries
)

, positioned as (
    select
        first_entries.*
        , required_programs.program_display_mode as child_display_mode
        , row_number() over (
            partition by first_entries.program_id
            order by first_entries.node_path, first_entries.nested_path
        ) - 1 as child_position
    from first_entries
    left join programs as required_programs
        on first_entries.child_resource_type = 'program' and first_entries.child_id = required_programs.program_id
    where
        first_entries.entry_rank = 1
        and (
            first_entries.child_resource_type = 'course'
            or coalesce(required_programs.program_display_mode, '') = 'course'
        )
)

-- The ETL loads a program's course only when the course's page is live
, live_courses as (
    select
        courses.course_id
        , courses.course_readable_id
    from {{ ref('stg__mitxonline__app__postgres__courses_course') }} as courses
    inner join {{ ref('int__mitxonline__catalog_pages') }} as pages on courses.course_id = pages.course_id
    where pages.page_is_live and upper(courses.course_title) not like '%PROCTORED EXAM%'
)

select
    programs.program_readable_id as readable_id
    , 'course' as child_resource_type
    , live_courses.course_readable_id as child_readable_id
    , cast(null as varchar) as child_display_mode
    , positioned.child_position
from positioned
inner join programs on positioned.program_id = programs.program_id
inner join live_courses on positioned.child_id = live_courses.course_id
where positioned.child_resource_type = 'course'
union all
-- Every required program is a child program. Only one shown as a course has a position.
select
    programs.program_readable_id as readable_id
    , 'program' as child_resource_type
    , program_nodes.required_program_readable_id as child_readable_id
    , program_nodes.required_program_display_mode as child_display_mode
    , positioned.child_position
from program_nodes
inner join programs on program_nodes.program_id = programs.program_id
left join positioned
    on
        program_nodes.program_id = positioned.program_id
        and program_nodes.required_program_id = positioned.child_id
        and positioned.child_resource_type = 'program'
where program_nodes.node_rank = 1
