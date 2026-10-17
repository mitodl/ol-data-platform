{#
    Which known concepts each courseware block or course document mentions, one row
    per (source, concept), from dictionary tagging of int__courseware__extraction_sources
    with dim_concept_alias. Mentions only, not teaching status: bridge_courserun_concept
    has what a run teaches, uses or requires.

    is_ambiguous is true when every mention came through an ambiguous phrase (short, a
    head without its qualifier, a single lowercase word such as "data", or shared by
    several concepts). Where a wrong tag costs more than a missed one, filter on it or
    require is_named_in_scope or is_cited_by_extractor.
#}
with tags as (
    select * from {{ ref('int__courseware__concept_tags') }}
)

, cited_sources as (
    select * from {{ ref('int__courseware__cited_concept_sources') }}
)

, named_in_scope as (
    select distinct
        platform
        , courserun_readable_id
        , scope_id
        , concept_id
    from {{ ref('int__courseware__resolved_concept_mentions') }}
)

, content as (
    select * from {{ ref('dim_course_content') }}
    where is_latest
)

, source_concepts as (
    select
        tagging_run_id
        , platform
        , courserun_readable_id
        , scope_id
        , scope_kind
        , source_ref
        , source_kind
        , concept_id
        , count(*) as mention_count
        , count(distinct pattern_key) as phrase_count
        , min(char_start) as first_char_start
        , min(case when is_ambiguous then 1 else 0 end) = 1 as is_ambiguous
    from tags
    group by
        tagging_run_id
        , platform
        , courserun_readable_id
        , scope_id
        , scope_kind
        , source_ref
        , source_kind
        , concept_id
)

select
    {{ dbt_utils.generate_surrogate_key([
        'source_concepts.platform', 'source_concepts.courserun_readable_id',
        'source_concepts.source_ref', 'source_concepts.concept_id'
    ]) }} as course_content_concept_pk
    , source_concepts.platform
    , source_concepts.courserun_readable_id
    -- the block, when the source is a block rather than an exported file
    , content.content_block_pk as content_block_fk
    , source_concepts.concept_id as concept_fk
    , source_concepts.scope_id
    , source_concepts.scope_kind
    , source_concepts.source_ref
    , source_concepts.source_kind
    , source_concepts.mention_count
    , source_concepts.phrase_count
    , source_concepts.first_char_start
    , source_concepts.is_ambiguous
    , cited_sources.source_ref is not null as is_cited_by_extractor
    , named_in_scope.concept_id is not null as is_named_in_scope
    , source_concepts.tagging_run_id
from source_concepts
left join content
    on
        source_concepts.platform = content.platform
        and source_concepts.courserun_readable_id = content.courserun_readable_id
        and source_concepts.source_ref = content.block_id
left join cited_sources
    on
        source_concepts.platform = cited_sources.platform
        and source_concepts.courserun_readable_id = cited_sources.courserun_readable_id
        and source_concepts.scope_id = cited_sources.scope_id
        and source_concepts.source_ref = cited_sources.source_ref
        and source_concepts.concept_id = cited_sources.concept_id
left join named_in_scope
    on
        source_concepts.platform = named_in_scope.platform
        and source_concepts.courserun_readable_id = named_in_scope.courserun_readable_id
        and source_concepts.scope_id = named_in_scope.scope_id
        and source_concepts.concept_id = named_in_scope.concept_id
