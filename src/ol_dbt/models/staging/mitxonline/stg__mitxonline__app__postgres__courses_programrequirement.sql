-- MITx Online Program Requirement Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__courses_programrequirement') }}
)

{{ deduplicate_raw_table(raw_table='raw__mitxonline__app__postgres__courses_programrequirement', partition_columns='id') }}

-- A node deleted in MITx Online stays in raw when the loader does not carry deletes, and
-- treebeard gives its path to another node. Paths are unique in the source, so two rows with
-- one path are a deleted node and a live one. The higher id is kept: that is the live node
-- when the path went to a node added later, which is every such pair in production raw on
-- 2026-10-13. It is the wrong one when the path went to an older sibling moved up, and a
-- deleted node whose path was not reused is still here. Raw has no loader column common to
-- Airbyte and dlt to order by, so loading this table by full refresh is the real fix.
, current_nodes as (
    select
        *
        , row_number() over (partition by path order by id desc) as path_rank
    from most_recent_source
)

, cleaned as (
    select
        id as programrequirement_id
        , course_id
        , required_program_id
        , program_id
        , path as programrequirement_path
        , depth as programrequirement_depth
        , node_type as programrequirement_node_type
        , numchild as programrequirement_numchild
        , title as programrequirement_title
        , operator as programrequirement_operator
        , cast(operator_value as INTEGER) as programrequirement_operator_value
    from current_nodes
    where path_rank = 1
)

select * from cleaned
