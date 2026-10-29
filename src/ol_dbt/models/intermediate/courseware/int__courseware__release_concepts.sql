{#
    The concepts the next knowledge release would publish, built from the newest
    run of each stage. The courseware_knowledge_release asset copies these rows into
    a release, and integrations__learn__concepts reads them from the published one,
    never from here.

    A concept is here when a reviewer accepted it, it is active, and the current
    claims of at least one source-eligible run name it. Its displayed label, kind
    and course counts come from its own mentions in eligible runs only, so a run under an ineligible
    source changes nothing a learner sees, and course counts come only from
    relations an accepted claim supports (#3021's gate: accepting a concept does not
    accept the claims naming it) and, for an inferred requirement, that pass the
    support rule (#3028). The description is the newest one written
    under the prompt version courseware_description_approval approves. A concept
    the describer has inputs for but no such description yet is awaiting one, and
    the release asset refuses a candidate with any, or with a description written
    from inputs that have since changed (description_input_checksum). A concept with no description
    inputs (no eligible claim describes it) takes no description, even one written
    while an eligible run described it, so it is released with null descriptions
    and no learner-facing view shows it.
#}
with concepts as (
    select * from {{ ref('dim_concept') }}
    where
        lifecycle_status = 'active'
        and review_status = 'accepted'
)

, eligible_runs as (
    select * from {{ ref('int__courseware__source_eligibility') }}
    where is_source_eligible
)

, eligible_mentions as (
    select
        mentions.concept_id
        , mentions.concept_label_key
        , mentions.label
        , mentions.concept_kind
    from {{ ref('int__courseware__resolved_concept_mentions') }} as mentions
    inner join eligible_runs
        on
            mentions.platform = eligible_runs.platform
            and mentions.courserun_readable_id = eligible_runs.courserun_readable_id
)

-- The label key eligible runs mention most, as dim_concept picks over every run,
-- then that key's surface form and the concept's kind as its eligible mentions use
-- them most. Both are counted over this concept's eligible mentions, not taken from
-- int__courseware__concept_labels, which counts every run and every sense of a label.
, display_keys as (
    select
        concept_id
        , concept_label_key
    from (
        select
            concept_id
            , concept_label_key
            , row_number() over (
                partition by concept_id order by count(*) desc, concept_label_key asc
            ) as key_rank
        from eligible_mentions
        group by concept_id, concept_label_key
    ) as ranked
    where key_rank = 1
)

, display_labels as (
    select
        concept_id
        , label as preferred_label
    from (
        select
            eligible_mentions.concept_id
            , eligible_mentions.label
            , row_number() over (
                partition by eligible_mentions.concept_id order by count(*) desc, eligible_mentions.label asc
            ) as label_rank
        from eligible_mentions
        inner join display_keys
            on
                eligible_mentions.concept_id = display_keys.concept_id
                and eligible_mentions.concept_label_key = display_keys.concept_label_key
        group by eligible_mentions.concept_id, eligible_mentions.label
    ) as ranked
    where label_rank = 1
)

, display_kinds as (
    select
        concept_id
        , concept_kind
    from (
        select
            concept_id
            , concept_kind
            , row_number() over (
                partition by concept_id order by count(*) desc, concept_kind asc
            ) as kind_rank
        from eligible_mentions
        where concept_kind is not null
        group by concept_id, concept_kind
    ) as ranked
    where kind_rank = 1
)

, approval as (
    {{ courseware_approved_description_prompt() }}
)

, described_concepts as (
    select distinct concept_id
    from {{ ref('int__courseware__concept_description_inputs') }}
)

, descriptions as (
    select
        concept_id
        , short_description
        , description
        , is_learner_facing
        , model_id
        , prompt_version
        , input_checksum
    from (
        select
            descriptions.*
            , row_number() over (
                partition by descriptions.concept_id order by descriptions.described_at desc
            ) as description_rank
        from {{ source('courseware_intermediate', 'courseware_concept_description') }} as descriptions
        inner join approval
            on descriptions.prompt_version = approval.prompt_version
        -- a concept with no current inputs keeps no description, even one written
        -- when a run since made ineligible gave it some
        inner join described_concepts
            on descriptions.concept_id = described_concepts.concept_id
    ) as ranked
    where description_rank = 1
)

, eligible_bridge as (
    select bridge.*
    from {{ ref('bridge_courserun_concept') }} as bridge
    inner join eligible_runs
        on
            bridge.platform = eligible_runs.platform
            and bridge.courserun_readable_id = eligible_runs.courserun_readable_id
    -- accepted claims only, and for inferred requires the support rule as well
    where bridge.accepted_claim_count > 0 and bridge.passes_support_rule
)

, course_runs as (
    select * from {{ ref('dim_course_run') }}
    where is_current
)

, course_counts as (
    select
        eligible_bridge.concept_fk
        , count(distinct case when eligible_bridge.relation = 'teaches' then course_runs.course_fk end)
        as teaching_course_count
        , count(distinct course_runs.course_fk) as course_count
    from eligible_bridge
    inner join course_runs
        on eligible_bridge.courserun_fk = course_runs.courserun_pk
    group by eligible_bridge.concept_fk
)

select
    concepts.concept_pk as concept_id
    , display_labels.preferred_label as name
    , display_kinds.concept_kind
    , descriptions.short_description
    , descriptions.description
    , descriptions.is_learner_facing
    , concepts.review_status
    , descriptions.model_id as description_model_id
    , descriptions.prompt_version as description_prompt_version
    , descriptions.input_checksum as description_input_checksum
    , (
        described_concepts.concept_id is not null
        and descriptions.concept_id is null
    ) as is_awaiting_description
    , coalesce(course_counts.teaching_course_count, 0) as teaching_course_count
    , coalesce(course_counts.course_count, 0) as course_count
    , {{ courseware_build_id() }} as dbt_invocation_id
from concepts
inner join display_labels
    on concepts.concept_pk = display_labels.concept_id
left join display_kinds
    on concepts.concept_pk = display_kinds.concept_id
left join descriptions
    on concepts.concept_pk = descriptions.concept_id
left join described_concepts
    on concepts.concept_pk = described_concepts.concept_id
left join course_counts
    on concepts.concept_pk = course_counts.concept_fk
