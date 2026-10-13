-- The nodes of every MITx Online program's requirement tree, one per tree path. Raw keeps a
-- deleted node, because courses_programrequirement is replicated without deletes, and
-- treebeard gives a deleted node's path to the next node added in its place. Two rows with
-- one path are therefore a deleted node and its replacement, and the higher id is the
-- replacement. A deleted node whose path was not reused cannot be told from a live one here.

with nodes as (
    select
        *
        , row_number() over (
            partition by programrequirement_path
            order by programrequirement_id desc
        ) as path_rank
    from {{ ref('stg__mitxonline__app__postgres__courses_programrequirement') }}
)

select
    programrequirement_id
    , program_id
    , course_id
    , required_program_id
    , programrequirement_node_type
    , programrequirement_path
    , programrequirement_depth
from nodes
where path_rank = 1
