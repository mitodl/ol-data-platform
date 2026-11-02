{{ config(enabled=courseware_local_tables_enabled('courseware_tagging_in_warehouse')) }}

{#
    The newest tagging run's dictionary, one row per (phrase, concept the run tagged
    it with), with the concept as of the newest resolution run (concept_id follows
    the redirect of a concept merged since the tagging run; tagged_concept_id is the
    id as tagged).

    has_multiple_concepts and is_ambiguous are recomputed from the current concepts,
    not copied from the run: a phrase the run gave to two concepts that have since
    merged belongs to one concept now. The other flags depend only on the phrase.
    These flags are review signals for the dictionary; they do not decide which
    concept a mention means (int__courseware__concept_tags' sense_status does).
#}
with runs as (
    select * from {{ source('courseware_intermediate', 'courseware_concept_tagging_run') }}
)

, latest_run as (
    select tagging_run_id
    from (
        select
            tagging_run_id
            , row_number() over (order by tagged_at desc) as run_rank
        from runs
    ) as ranked
    where run_rank = 1
)

, registry as (
    select * from {{ ref('int__courseware__concept_registry') }}
)

, aliases as (
    select
        alias.tagging_run_id
        , alias.pattern_key
        , coalesce(registry.merged_into_concept_id, alias.concept_id) as concept_id
        , alias.concept_id as tagged_concept_id
        , alias.concept_label_key
        , alias.variant
        , alias.surface
        , alias.match_mode
        , alias.is_short
        , alias.is_qualified
        , alias.is_single_word
    from {{ source('courseware_intermediate', 'courseware_concept_tagging_alias') }} as alias
    inner join latest_run
        on alias.tagging_run_id = latest_run.tagging_run_id
    left join registry
        on alias.concept_id = registry.concept_id
)

, phrase_concepts as (
    select
        pattern_key
        , count(distinct concept_id) as concept_count
    from aliases
    group by pattern_key
)

select
    aliases.tagging_run_id
    , aliases.pattern_key
    , aliases.concept_id
    , aliases.tagged_concept_id
    , aliases.concept_label_key
    , aliases.variant
    , aliases.surface
    , aliases.match_mode
    , aliases.is_short
    , aliases.is_qualified
    , aliases.is_single_word
    , phrase_concepts.concept_count > 1 as has_multiple_concepts
    , (
        aliases.is_short
        or aliases.is_qualified
        or aliases.is_single_word
        or phrase_concepts.concept_count > 1
    ) as is_ambiguous
from aliases
inner join phrase_concepts
    on aliases.pattern_key = phrase_concepts.pattern_key
