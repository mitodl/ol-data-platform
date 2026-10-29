{{ config(severity="warn") }}

-- Dictionary tagging repeats each phrase's concept at every occurrence of the phrase,
-- so one wrong dictionary entry (a bad merge that put a common phrase under a concept,
-- or a generic word the ambiguity flags missed) shows up as a phrase producing a large
-- share of a run's mentions. A row here is an unflagged phrase above 5%; check its
-- assignment in int__courseware__concept_tagging_fan_out and dim_concept_alias.
select
    pattern_key
    , concept_id
    , concept_label_key
    , tagged_mention_count
    , share_of_tagged_mentions
    , scope_agreement_rate
from {{ ref('int__courseware__concept_tagging_fan_out') }}
where
    not is_ambiguous
    and share_of_tagged_mentions > 0.05
