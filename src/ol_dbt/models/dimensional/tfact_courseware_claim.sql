-- One row per extracted claim, as the model stated it. Labels and descriptions are
-- never edited here; concept_fk is the only resolved column, the sense of the label
-- the claim's course run uses. Every claim is
-- 'proposed' until a reviewer accepts or rejects it; accepting the concept a claim
-- names does not accept the claim.
with claims as (
    select * from {{ ref('int__courseware__claims') }}
)

, extractions as (
    select * from {{ ref('dim_courseware_extraction') }}
)

, sense_assignments as (
    select * from {{ ref('int__courseware__concept_sense_assignments') }}
    where sense_status = 'assigned'
)

select
    {{ dbt_utils.generate_surrogate_key(['claims.extraction_key', 'claims.claim_ordinal']) }} as courseware_claim_pk
    , extractions.courseware_extraction_pk as courseware_extraction_fk
    , claims.platform
    , claims.courserun_readable_id
    , extractions.courserun_fk
    , extractions.scope_block_fk
    , claims.scope_kind
    , claims.scope_position
    , claims.claim_ordinal
    , claims.claim_kind
    , claims.label
    , claims.description
    , claims.concept_kind
    , claims.teaching_status
    , claims.how_used
    , claims.flagged_as_prior
    , claims.bloom_process
    , claims.knowledge_type
    , claims.prerequisite_kind
    , claims.course_ref
    , claims.related_concept_labels_json
    , sense_assignments.concept_id as concept_fk
    , claims.evidence_count
    , claims.verified_evidence_count
    , claims.is_verified
    , claims.is_current
    , claims.review_status
    , claims.extracted_at
from claims
inner join extractions
    on claims.extraction_key = extractions.extraction_key
left join sense_assignments
    on
        claims.concept_label_key = sense_assignments.concept_label_key
        and claims.platform = sense_assignments.platform
        and claims.courserun_readable_id = sense_assignments.courserun_readable_id
