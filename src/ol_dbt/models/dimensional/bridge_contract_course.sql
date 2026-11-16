-- Where each course sits in a MITx Online B2B contract: by the contract's program list,
-- then by the program's requirement tree, the order the learner dashboard shows.
-- Grain: one row per (contract, course) for courses in at least one of the contract's
-- programs. A course in several of them takes its lowest (program, course) position.
-- shortcut: the raw contract program items come from Airbyte's xmin cursor, which never
-- carries a hard delete, so a program removed from a contract keeps its position here;
-- upgrade when that table is loaded with deletes.
-- shortcut: bridge_program_course holds only a program's own course nodes, so a course
-- reached only through a program nested in a contract program has no row; upgrade when
-- the bridge expands nested programs.
with contract_programs as (
    select
        contract_id
        , program_id
        , contractprogramitem_sort_order
    from {{ ref('stg__mitxonline__app__postgres__b2b_contractprogramitem') }}
)

, dim_contract as (
    select contract_pk, contract_id
    from {{ ref('dim_contract') }}
)

, dim_program as (
    select program_pk, source_id
    from {{ ref('dim_program') }}
    where platform_code = 'mitxonline'
)

, bridge_program_course as (
    select program_fk, course_fk, course_order
    from {{ ref('bridge_program_course') }}
)

, contract_courses as (
    select
        dim_contract.contract_pk as contract_fk
        , bridge_program_course.course_fk
        , dim_program.program_pk as program_fk
        , contract_programs.contractprogramitem_sort_order as program_sort_order
        , bridge_program_course.course_order as course_order_in_program
        , row_number() over (
            partition by dim_contract.contract_pk, bridge_program_course.course_fk
            order by
                contract_programs.contractprogramitem_sort_order asc nulls last
                , bridge_program_course.course_order asc nulls last
                , dim_program.program_pk asc nulls last
        ) as position_rank
    from contract_programs
    inner join dim_contract
        on contract_programs.contract_id = dim_contract.contract_id
    inner join dim_program
        on cast(contract_programs.program_id as varchar) = dim_program.source_id
    inner join bridge_program_course
        on dim_program.program_pk = bridge_program_course.program_fk
)

select
    contract_fk
    , course_fk
    , program_fk
    , program_sort_order
    , course_order_in_program
from contract_courses
where position_rank = 1
