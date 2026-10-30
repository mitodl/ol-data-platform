-- Every concept mention with the concept its course run's sense of the label is.
-- Mentions whose (label, course run) the newest resolution run has not assigned, or
-- has queued for review, are left out until it is assigned.
with mentions as (
    select * from {{ ref('int__courseware__concept_mentions') }}
)

, sense_assignments as (
    select * from {{ ref('int__courseware__concept_sense_assignments') }}
    where sense_status = 'assigned'
)

select
    mentions.platform
    , mentions.courserun_readable_id
    , mentions.scope_id
    , mentions.scope_kind
    , mentions.scope_position
    , mentions.extraction_key
    , mentions.claim_ordinal
    , mentions.extracted_at
    , mentions.claim_kind
    , mentions.teaching_status
    , mentions.concept_kind
    , mentions.description
    , mentions.label
    , mentions.concept_label_key
    , sense_assignments.concept_id
    , mentions.prerequisite_kind
    , mentions.review_status
from mentions
inner join sense_assignments
    on
        mentions.concept_label_key = sense_assignments.concept_label_key
        and mentions.platform = sense_assignments.platform
        and mentions.courserun_readable_id = sense_assignments.courserun_readable_id
