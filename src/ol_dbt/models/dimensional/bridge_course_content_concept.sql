{{ config(enabled=courseware_local_tables_enabled('courseware_tagging_in_warehouse')) }}

{#
    Which known concepts each courseware block or course document mentions, one row
    per (source, concept), from dictionary tagging of int__courseware__extraction_sources
    with dim_concept_alias. Mentions only, not teaching status: bridge_courserun_concept
    has what a run teaches, uses or requires.

    Every row is a mention candidate. is_sense_resolved is true when the course run's
    sense of the label is this concept, i.e. the extractor named the label in the run
    and sense assignment chose this concept; otherwise nothing has decided which of
    the label's concepts the text means here, even for a label with one known sense.
    Where the concept must be right, require is_sense_resolved.

    is_ambiguous is true when every mention came through an ambiguous phrase (short, a
    head without its qualifier, a single common word such as "data", or shared by
    several concepts). It is a review signal for the dictionary, not a sense decision.
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
        , max(case when sense_status = 'course_sense' then 1 else 0 end) = 1 as is_sense_resolved
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
    , source_concepts.is_sense_resolved
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
