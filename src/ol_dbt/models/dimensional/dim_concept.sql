-- The concepts extracted claims name, across every course run. concept_pk is the
-- opaque UUID the resolver minted, used as is rather than hashed from a natural key:
-- it is the identity, and nothing about a concept (its label included) derives it.
-- Merged and deprecated concepts keep their rows, with merged_into_concept_fk as the
-- redirect. Every row is 'proposed' until a reviewer accepts, merges or rejects it.
--
-- Two descriptions, for two audiences: extracted_description is one claim's account
-- of how a scope treats the concept (evidence, course-specific); description and
-- short_description are written once per concept for learner-facing surfaces.
-- The displayed label is the most-mentioned member label, chosen here at read time,
-- so it can change without the id changing.
with registry as (
    select * from {{ ref('int__courseware__concept_registry') }}
)

, assignments as (
    select * from {{ ref('int__courseware__concept_assignments') }}
    where concept_id is not null
)

, labels as (
    select * from {{ ref('int__courseware__concept_labels') }}
)

, mentions as (
    select * from {{ ref('int__courseware__concept_mentions') }}
)

, descriptions as (
    select * from {{ source('courseware_intermediate', 'courseware_concept_description') }}
)

, latest_descriptions as (
    select
        concept_id
        , short_description
        , description
        , is_learner_facing
        , model_id
        , described_at
    from (
        select
            *
            , row_number() over (partition by concept_id order by described_at desc) as description_rank
        from descriptions
    ) as ranked
    where description_rank = 1
)

, members as (
    select
        assignments.concept_id
        , labels.concept_label_key
        , labels.preferred_label
        , labels.concept_kind
        , labels.description
        , labels.mention_count
        , labels.taught_mention_count
        , row_number() over (
            partition by assignments.concept_id order by labels.mention_count desc, labels.concept_label_key asc
        ) as member_rank
    from assignments
    inner join labels
        on assignments.concept_label_key = labels.concept_label_key
)

, concept_runs as (
    select
        assignments.concept_id
        , count(distinct concat(mentions.platform, '|', mentions.courserun_readable_id)) as courserun_count
    from mentions
    inner join assignments
        on mentions.concept_label_key = assignments.concept_label_key
    group by assignments.concept_id
)

, member_stats as (
    select
        concept_id
        , max(case when member_rank = 1 then preferred_label end) as preferred_label
        , max(case when member_rank = 1 then concept_kind end) as concept_kind
        , max(case when member_rank = 1 then description end) as extracted_description
        , count(*) as label_count
        , sum(mention_count) as mention_count
        , sum(taught_mention_count) as taught_mention_count
    from members
    group by concept_id
)

select
    registry.concept_id as concept_pk
    , coalesce(member_stats.preferred_label, registry.minted_label) as preferred_label
    , member_stats.concept_kind
    , latest_descriptions.short_description
    , latest_descriptions.description
    , latest_descriptions.is_learner_facing
    , latest_descriptions.model_id as description_model_id
    , latest_descriptions.described_at
    , member_stats.extracted_description
    , registry.concept_status as lifecycle_status
    , registry.merged_into_concept_id as merged_into_concept_fk
    , 'proposed' as review_status
    , cast(null as varchar) as wikidata_qid
    , registry.minted_label
    , registry.minted_run_id
    , registry.minted_at
    , coalesce(member_stats.label_count, 0) as label_count
    , coalesce(member_stats.mention_count, 0) as mention_count
    , coalesce(concept_runs.courserun_count, 0) as courserun_count
    , coalesce(member_stats.taught_mention_count, 0) as taught_mention_count
from registry
left join member_stats
    on registry.concept_id = member_stats.concept_id
left join concept_runs
    on registry.concept_id = concept_runs.concept_id
left join latest_descriptions
    on registry.concept_id = latest_descriptions.concept_id
