-- One row per extracted claim, as the model stated it. Labels and descriptions are
-- never edited here.
with claims as (
    select * from {{ ref('int__courseware__claims') }}
)

, extractions as (
    select * from {{ ref('dim_courseware_extraction') }}
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
    , claims.evidence_count
    , claims.verified_evidence_count
    , claims.is_verified
    , claims.is_current
    , claims.extracted_at
from claims
inner join extractions
    on claims.extraction_key = extractions.extraction_key
