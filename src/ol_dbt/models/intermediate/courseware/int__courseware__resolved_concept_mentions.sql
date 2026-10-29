-- Every concept mention with the concept its label is assigned to. Mentions whose
-- label the newest resolution run has not seen yet are left out until it has.
with mentions as (
    select * from {{ ref('int__courseware__concept_mentions') }}
)

, assignments as (
    select * from {{ ref('int__courseware__concept_assignments') }}
    where concept_id is not null
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
    , assignments.concept_id
    , mentions.prerequisite_kind
    , mentions.review_status
from mentions
inner join assignments
    on mentions.concept_label_key = assignments.concept_label_key
