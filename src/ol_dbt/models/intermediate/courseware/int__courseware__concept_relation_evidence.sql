{#
    Every observation that two concepts are related, one row per (concept pair,
    relation, evidence source, course run). Rows are rebuilt from the current claims
    and the newest resolution run on every build, so an edge gains support as courses
    are extracted and loses it when an extraction version stops being current.

    Relations, and what produces them:
      broader     resolution adjudication: from is a kind or part of to (hierarchy)
      close       resolution adjudication: nearly interchangeable (exact is a merge)
      related     resolution adjudication: connected, neither same nor kind-of
      builds_on   course_sequence: a unit that teaches `from` relies on `to` (assumes
                  it, uses it unexplained, or reviews it), and `to` is taught earlier
                  in the same course or not at all; so `from` builds on `to`
      practiced_with  the same, when `to` is a tool: learning `from` here involves
                  using the tool, which is not a step in knowledge order
      complements co_taught: both taught in the same unit
                  co_assessed: both named by the same assessment claim

    Directed relations (broader, builds_on) keep their direction; the others store
    the pair with from_concept_id < to_concept_id. Datasets are left out of course
    evidence: a course's example data is material, not knowledge.
#}
with mentions as (
    select * from {{ ref('int__courseware__resolved_concept_mentions') }}
    where coalesce(concept_kind, '') != 'dataset'
)

, adjudicated as (
    select * from {{ ref('int__courseware__adjudicated_concept_relations') }}
)

, taught as (
    select distinct
        platform
        , courserun_readable_id
        , scope_id
        , scope_position
        , concept_id
        , extracted_at
    from mentions
    where
        claim_kind = 'concept'
        and teaching_status = 'taught'
        and scope_kind = 'unit'
)

, relied_on as (
    select distinct
        platform
        , courserun_readable_id
        , scope_id
        , concept_id
    from mentions
    where
        scope_kind = 'unit'
        and (
            claim_kind = 'assumed_knowledge'
            or (claim_kind = 'concept' and teaching_status in ('used', 'reviewed'))
        )
)

, first_taught as (
    select
        platform
        , courserun_readable_id
        , concept_id
        , min(scope_position) as first_taught_position
    from taught
    group by platform, courserun_readable_id, concept_id
)

-- a concept's kind is the one most of its mentions give it
, concept_kinds as (
    select
        concept_id
        , concept_kind
    from (
        select
            concept_id
            , concept_kind
            , row_number() over (partition by concept_id order by count(*) desc, concept_kind) as kind_rank
        from mentions
        where concept_kind is not null
        group by concept_id, concept_kind
    ) as ranked
    where kind_rank = 1
)

, course_sequence as (
    select
        taught.concept_id as from_concept_id
        , relied_on.concept_id as to_concept_id
        -- relying on a tool is a practice dependency, not a step in learning order
        , case when concept_kinds.concept_kind = 'tool' then 'practiced_with' else 'builds_on' end as relation
        , 'course_sequence' as evidence_source
        , taught.platform
        , taught.courserun_readable_id
        , count(distinct taught.scope_id) as scope_count
        , min(taught.extracted_at) as observed_at
    from taught
    inner join relied_on
        on
            taught.platform = relied_on.platform
            and taught.courserun_readable_id = relied_on.courserun_readable_id
            and taught.scope_id = relied_on.scope_id
            and taught.concept_id != relied_on.concept_id
    left join first_taught
        on
            relied_on.platform = first_taught.platform
            and relied_on.courserun_readable_id = first_taught.courserun_readable_id
            and relied_on.concept_id = first_taught.concept_id
    left join concept_kinds
        on relied_on.concept_id = concept_kinds.concept_id
    where
        first_taught.first_taught_position is null
        or first_taught.first_taught_position < taught.scope_position
    group by
        taught.concept_id, relied_on.concept_id, concept_kinds.concept_kind
        , taught.platform, taught.courserun_readable_id
)

, co_taught as (
    select
        left_taught.concept_id as from_concept_id
        , right_taught.concept_id as to_concept_id
        , 'complements' as relation
        , 'co_taught' as evidence_source
        , left_taught.platform
        , left_taught.courserun_readable_id
        , count(distinct left_taught.scope_id) as scope_count
        , min(left_taught.extracted_at) as observed_at
    from taught as left_taught
    inner join taught as right_taught
        on
            left_taught.platform = right_taught.platform
            and left_taught.courserun_readable_id = right_taught.courserun_readable_id
            and left_taught.scope_id = right_taught.scope_id
            and left_taught.concept_id < right_taught.concept_id
    group by left_taught.concept_id, right_taught.concept_id, left_taught.platform, left_taught.courserun_readable_id
)

, assessed as (
    select distinct
        platform
        , courserun_readable_id
        , scope_id
        , extraction_key
        , claim_ordinal
        , concept_id
        , extracted_at
    from mentions
    where claim_kind = 'assessment_expectation'
)

, co_assessed as (
    select
        left_assessed.concept_id as from_concept_id
        , right_assessed.concept_id as to_concept_id
        , 'complements' as relation
        , 'co_assessed' as evidence_source
        , left_assessed.platform
        , left_assessed.courserun_readable_id
        , count(distinct left_assessed.scope_id) as scope_count
        , min(left_assessed.extracted_at) as observed_at
    from assessed as left_assessed
    inner join assessed as right_assessed
        on
            left_assessed.extraction_key = right_assessed.extraction_key
            and left_assessed.claim_ordinal = right_assessed.claim_ordinal
            and left_assessed.concept_id < right_assessed.concept_id
    group by
        left_assessed.concept_id, right_assessed.concept_id
        , left_assessed.platform, left_assessed.courserun_readable_id
)

, adjudication as (
    select
        from_concept_id
        , to_concept_id
        , relation
        , 'adjudication' as evidence_source
        , cast(null as varchar) as platform
        , cast(null as varchar) as courserun_readable_id
        , 0 as scope_count
        , proposed_at as observed_at
    from adjudicated
)

select * from course_sequence
union all
select * from co_taught
union all
select * from co_assessed
union all
select * from adjudication
