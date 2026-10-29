-- Every sense of every label: one row per (normalized label key, concept). A label
-- can name several concepts (its primary sense and the senses split off it), so the
-- label alone does not identify a concept; sense_gloss and the course run's sense
-- assignment do.
with aliases as (
    select * from {{ ref('int__courseware__concept_aliases') }}
)

, labels as (
    select * from {{ ref('int__courseware__concept_labels') }}
)

, sense_counts as (
    select
        concept_label_key
        , concept_id
        , count(*) as courserun_count
    from {{ ref('int__courseware__concept_sense_assignments') }}
    where sense_status = 'assigned'
    group by concept_label_key, concept_id
)

select
    {{ dbt_utils.generate_surrogate_key(['aliases.concept_label_key', 'aliases.concept_id']) }} as concept_alias_pk
    , aliases.concept_id as concept_fk
    , coalesce(labels.preferred_label, aliases.concept_label_key) as alias
    , aliases.concept_label_key
    , aliases.sense_role
    , aliases.sense_gloss
    , aliases.added_run_id
    , coalesce(sense_counts.courserun_count, 0) as courserun_count
from aliases
left join labels
    on aliases.concept_label_key = labels.concept_label_key
left join sense_counts
    on
        aliases.concept_label_key = sense_counts.concept_label_key
        and aliases.concept_id = sense_counts.concept_id
