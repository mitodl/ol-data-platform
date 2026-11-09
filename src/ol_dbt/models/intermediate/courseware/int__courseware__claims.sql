{#
    Every extracted claim with its extraction's scope and version, its evidence counts,
    and the key that groups its label with the same label elsewhere.

    is_current is int__courseware__extractions': a claim is current when its extraction
    is the one its scope part is read from now. A claim is verified when at least one
    of its passages was found in the source text.

    text_input_digest groups claims read from identical text, in any scope or run:
    they share one source, so rollups that count supporting claims should count
    such a group once rather than as independent corroboration.

    concept_label_key is a string normalization only (the courseware_label_key macro).
    Resolving synonyms and variants across courses ("k-means" and "k-means
    clustering") is a separate step that will map these keys onto dim_concept. A
    declared "no prerequisites" (prerequisite_kind explicitly_none) gets no key: its
    label names what is not required, so it is not a mention of that concept.

    review_status is 'proposed' for every claim until a review workflow persists
    decisions; this is where they will be joined in. Learner-facing views take only
    accepted claims.
#}
{%- set stored_claims = source('courseware_intermediate', 'courseware_claim') %}
with extractions as (
    select * from {{ ref('int__courseware__extractions') }}
)

-- text_teaching_status and sense_hint exist once the extraction asset first
-- writes a v2 claim
, claims as (
    select
        *
        {{- missing_columns_as_nulls(stored_claims, {
            'text_teaching_status': dbt.type_string(),
            'sense_hint': dbt.type_string(),
        }) }}
    from {{ stored_claims }}
)

, evidence as (
    select * from {{ source('courseware_intermediate', 'courseware_claim_evidence') }}
)

, evidence_counts as (
    select
        extraction_key
        , claim_ordinal
        , count(*) as evidence_count
        , count(case when verification_status in ('exact', 'fuzzy') then 1 end) as verified_evidence_count
    from evidence
    group by extraction_key, claim_ordinal
)

select
    extractions.platform
    , extractions.courserun_readable_id
    , extractions.scope_id
    , extractions.scope_kind
    , extractions.scope_title
    , extractions.scope_position
    , extractions.scope_part
    , extractions.extraction_key
    , extractions.extractor_id
    , extractions.model_id
    , extractions.prompt_version
    , extractions.extracted_at
    , extractions.text_input_digest
    , extractions.is_current
    , claims.claim_ordinal
    , claims.claim_kind
    , claims.label
    , claims.description
    , claims.concept_kind
    , claims.teaching_status
    , claims.text_teaching_status
    , claims.sense_hint
    , claims.how_used
    , claims.flagged_as_prior
    , claims.bloom_process
    , claims.knowledge_type
    , claims.prerequisite_kind
    , claims.course_ref
    , claims.related_concept_labels_json
    , case
        when
            claims.claim_kind in ('concept', 'assumed_knowledge', 'declared_prerequisite')
            and coalesce(claims.prerequisite_kind, '') != 'explicitly_none'
            then {{ courseware_label_key('claims.label') }}
    end as concept_label_key
    , coalesce(evidence_counts.evidence_count, 0) as evidence_count
    , coalesce(evidence_counts.verified_evidence_count, 0) as verified_evidence_count
    , coalesce(evidence_counts.verified_evidence_count, 0) > 0 as is_verified
    , 'proposed' as review_status
from claims
inner join extractions
    on claims.extraction_key = extractions.extraction_key
left join evidence_counts
    on
        claims.extraction_key = evidence_counts.extraction_key
        and claims.claim_ordinal = evidence_counts.claim_ordinal
