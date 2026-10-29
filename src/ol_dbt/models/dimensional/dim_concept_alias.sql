-- Every label a concept was extracted under: the index resolution and dictionary
-- propagation match against. One row per normalized label key with an assignment.
with assignments as (
    select * from {{ ref('int__courseware__concept_assignments') }}
    where concept_id is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['concept_label_key']) }} as concept_alias_pk
    , concept_id as concept_fk
    , preferred_label as alias
    , concept_label_key
    , assignment_source
    , assigned_run_id
    , mention_count
    , courserun_count
from assignments
