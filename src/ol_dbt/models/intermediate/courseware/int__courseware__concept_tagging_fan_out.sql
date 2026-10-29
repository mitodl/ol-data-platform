{#
    What each dictionary phrase of the newest tagging run produced, and how often the
    extractor agrees, one row per (phrase, concept). This is the review queue for the
    dictionary: a wrong entry is repeated at every occurrence of its phrase, so the
    phrases with the most tagged sources are the ones to check first.

    Agreement is measured on tagged sources in scopes with a current extraction:
    source agreement when a claim naming the concept quotes that source, scope
    agreement when any claim in the scope names it. A phrase with many tagged sources
    and little agreement is tagging a sense the extractor does not connect to the
    concept (a generic word, or a wrong entry). A wrong merge is not caught this way,
    because the extractor's own mentions of a merged label resolve through the same
    merge. is_merged_label and amplification (tagged scopes per scope where the
    extractor used the label itself) show how far tagging spreads each merge decision.

    The run's dictionary rows carry the concept ids current when it ran;
    concept_id follows the newest resolution run's redirects, which is what the
    extractor's mentions and the label counts are keyed by. Dictionary rows of one
    phrase whose concepts have since merged become one row, under the lowest
    tagged_concept_id, as int__courseware__concept_tags keeps their mentions.
#}
with tags as (
    select * from {{ ref('int__courseware__concept_tags') }}
)

, aliases as (
    -- dictionary rows of one phrase whose concepts have since merged fold into one
    select *
    from (
        select
            *
            , row_number() over (
                partition by pattern_key, concept_id
                order by tagged_concept_id
            ) as concept_rank
        from {{ ref('int__courseware__concept_tagging_aliases') }}
    ) as ranked
    where concept_rank = 1
)

, assignments as (
    select * from {{ ref('int__courseware__concept_assignments') }}
    where concept_id is not null
)

, concept_label_counts as (
    select
        concept_id
        , count(*) as label_count
    from assignments
    group by concept_id
)

, mentions as (
    select * from {{ ref('int__courseware__resolved_concept_mentions') }}
)

-- from the extractions, not the claims, so a scope whose current extraction found no
-- claims still counts its tagged sources as compared (and disagreeing)
, extracted_scopes as (
    select distinct
        platform
        , courserun_readable_id
        , scope_id
    from {{ ref('int__courseware__extractions') }}
    where is_current
)

, named_in_scope as (
    select distinct
        platform
        , courserun_readable_id
        , scope_id
        , concept_id
    from mentions
)

, label_scopes as (
    select
        concept_label_key
        , count(distinct concat(platform, '|', courserun_readable_id, '|', scope_id)) as extractor_scope_count
    from mentions
    group by concept_label_key
)

, cited_sources as (
    select * from {{ ref('int__courseware__cited_concept_sources') }}
)

, tagged_sources as (
    select
        tags.pattern_key
        , tags.concept_id
        , tags.platform
        , tags.courserun_readable_id
        , tags.scope_id
        , tags.source_ref
        , count(*) as mention_count
        , max(case when extracted_scopes.scope_id is not null then 1 else 0 end) as is_compared
        , max(case when cited_sources.source_ref is not null then 1 else 0 end) as is_cited
        , max(case when named_in_scope.scope_id is not null then 1 else 0 end) as is_named_in_scope
    from tags
    left join extracted_scopes
        on
            tags.platform = extracted_scopes.platform
            and tags.courserun_readable_id = extracted_scopes.courserun_readable_id
            and tags.scope_id = extracted_scopes.scope_id
    left join cited_sources
        on
            tags.platform = cited_sources.platform
            and tags.courserun_readable_id = cited_sources.courserun_readable_id
            and tags.scope_id = cited_sources.scope_id
            and tags.source_ref = cited_sources.source_ref
            and tags.concept_id = cited_sources.concept_id
    left join named_in_scope
        on
            tags.platform = named_in_scope.platform
            and tags.courserun_readable_id = named_in_scope.courserun_readable_id
            and tags.scope_id = named_in_scope.scope_id
            and tags.concept_id = named_in_scope.concept_id
    group by
        tags.pattern_key
        , tags.concept_id
        , tags.platform
        , tags.courserun_readable_id
        , tags.scope_id
        , tags.source_ref
)

, phrase_stats as (
    select
        pattern_key
        , concept_id
        , sum(mention_count) as tagged_mention_count
        , count(*) as tagged_source_count
        , count(distinct concat(platform, '|', courserun_readable_id, '|', scope_id)) as tagged_scope_count
        , count(distinct concat(platform, '|', courserun_readable_id)) as tagged_courserun_count
        , sum(is_compared) as compared_source_count
        , sum(is_compared * is_cited) as cited_source_count
        , sum(is_compared * is_named_in_scope) as named_in_scope_source_count
    from tagged_sources
    group by pattern_key, concept_id
)

, totals as (
    select sum(tagged_mention_count) as all_mention_count
    from phrase_stats
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
    , aliases.has_multiple_concepts
    , aliases.is_ambiguous
    , coalesce(concept_label_counts.label_count, 0) > 1 as is_merged_label
    , coalesce(phrase_stats.tagged_mention_count, 0) as tagged_mention_count
    , coalesce(phrase_stats.tagged_source_count, 0) as tagged_source_count
    , coalesce(phrase_stats.tagged_scope_count, 0) as tagged_scope_count
    , coalesce(phrase_stats.tagged_courserun_count, 0) as tagged_courserun_count
    , cast(coalesce(phrase_stats.tagged_mention_count, 0) as double)
    / nullif(totals.all_mention_count, 0) as share_of_tagged_mentions
    , coalesce(label_scopes.extractor_scope_count, 0) as extractor_scope_count
    , cast(phrase_stats.tagged_scope_count as double)
    / nullif(label_scopes.extractor_scope_count, 0) as amplification
    , coalesce(phrase_stats.compared_source_count, 0) as compared_source_count
    , cast(phrase_stats.cited_source_count as double)
    / nullif(phrase_stats.compared_source_count, 0) as source_agreement_rate
    , cast(phrase_stats.named_in_scope_source_count as double)
    / nullif(phrase_stats.compared_source_count, 0) as scope_agreement_rate
from aliases
cross join totals
left join phrase_stats
    on
        aliases.pattern_key = phrase_stats.pattern_key
        and aliases.concept_id = phrase_stats.concept_id
left join concept_label_counts
    on aliases.concept_id = concept_label_counts.concept_id
left join label_scopes
    on aliases.concept_label_key = label_scopes.concept_label_key
