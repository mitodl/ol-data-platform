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
-- so it can change without the id changing. Members, mentions and descriptions come
-- through sense assignment, so a concept split off a label for another meaning
-- (split_from_concept_fk) counts only the course runs that use it in that sense.
with registry as (
    select * from {{ ref('int__courseware__concept_registry') }}
)

, aliases as (
    select * from {{ ref('int__courseware__concept_aliases') }}
)

, labels as (
    select * from {{ ref('int__courseware__concept_labels') }}
)

, mentions as (
    select * from {{ ref('int__courseware__resolved_concept_mentions') }}
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

, member_mentions as (
    select
        concept_id
        , concept_label_key
        , count(*) as mention_count
        , count(case when teaching_status in ('taught', 'reviewed') then 1 end) as taught_mention_count
    from mentions
    group by concept_id, concept_label_key
)

, members as (
    select
        aliases.concept_id
        , labels.concept_label_key
        , labels.preferred_label
        , coalesce(member_mentions.mention_count, 0) as mention_count
        , coalesce(member_mentions.taught_mention_count, 0) as taught_mention_count
        , row_number() over (
            partition by aliases.concept_id
            order by coalesce(member_mentions.mention_count, 0) desc, labels.concept_label_key asc
        ) as member_rank
    from aliases
    inner join labels
        on aliases.concept_label_key = labels.concept_label_key
    left join member_mentions
        on
            aliases.concept_id = member_mentions.concept_id
            and aliases.concept_label_key = member_mentions.concept_label_key
)

, member_stats as (
    select
        concept_id
        , max(case when member_rank = 1 then preferred_label end) as preferred_label
        -- a label whose current mentions all went to another sense does not count, so
        -- a concept no current claim names has none and Learn leaves it out
        , count(case when mention_count > 0 then 1 end) as label_count
        , sum(mention_count) as mention_count
        , sum(taught_mention_count) as taught_mention_count
    from members
    group by concept_id
)

-- the kind and claim description of the concept's own mentions, not of every sense
-- of its labels. The kind is the most common one: assumed-knowledge claims have a
-- description but no kind, so the kind is not taken from the description's claim.
, mention_kinds as (
    select
        concept_id
        , concept_kind
        , row_number() over (
            partition by concept_id
            order by count(*) desc, concept_kind asc
        ) as kind_rank
    from mentions
    where concept_kind is not null
    group by concept_id, concept_kind
)

-- a taught description says what the concept is better than a used one
, mention_profiles as (
    select
        concept_id
        , description
        , row_number() over (
            partition by concept_id
            order by case when teaching_status = 'taught' then 0 else 1 end, length(description) desc
        ) as description_rank
    from mentions
    where description is not null
)

, concept_runs as (
    select
        concept_id
        , count(distinct concat(platform, '|', courserun_readable_id)) as courserun_count
    from mentions
    group by concept_id
)

select
    registry.concept_id as concept_pk
    , coalesce(member_stats.preferred_label, registry.minted_label) as preferred_label
    , mention_kinds.concept_kind
    , latest_descriptions.short_description
    , latest_descriptions.description
    , latest_descriptions.is_learner_facing
    , latest_descriptions.model_id as description_model_id
    , latest_descriptions.described_at
    , mention_profiles.description as extracted_description
    , registry.concept_status as lifecycle_status
    , registry.merged_into_concept_id as merged_into_concept_fk
    , registry.split_from_concept_id as split_from_concept_fk
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
left join mention_kinds
    on
        registry.concept_id = mention_kinds.concept_id
        and mention_kinds.kind_rank = 1
left join mention_profiles
    on
        registry.concept_id = mention_profiles.concept_id
        and mention_profiles.description_rank = 1
