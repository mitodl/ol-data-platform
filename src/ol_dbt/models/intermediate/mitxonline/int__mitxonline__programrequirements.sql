-- Program Requirement for MITx Online


with programrequirement as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__courses_programrequirement') }}
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
