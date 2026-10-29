{#
    Every place a known concept's label appears in courseware text, as of the newest
    dictionary tagging run (ml.lib.courseware.tagging over int__courseware__extraction_sources
    and dim_concept_alias), with the matched phrase's mode and ambiguity flags. These
    are mention candidates only. Whether a source teaches the concept is the
    extractor's call.

    A run tags with the concept ids current when it ran. concept_id follows the
    newest resolution run's redirects (a concept merged since then becomes its
    survivor), so it joins to the extractor's resolved mentions; tagged_concept_id
    is the id as tagged, which the run's dictionary rows are keyed by. A phrase the
    run gave to two concepts that have since merged tagged each of its spans twice,
    so one row per span and current concept is kept: a resolved one first, then the
    lowest tagged_concept_id.

    A label can name several concepts (its senses), and the dictionary emits a
    candidate for each. Which one a mention means is the course run's sense of the
    label (int__courseware__concept_sense_assignments), never the dictionary's:
    - course_sense: the run's sense of the label is this concept, or the run never
      used the label but gave another label to this concept and to none of the
      phrase's other candidates (the dictionary records one label per phrase and
      concept, e.g. "CSV" from "comma-separated values (CSV)" but not from "CSV file
      (CSV)");
    - needs_review: the run's sense of the label is in the review queue;
    - candidate: nothing has decided the sense here: the extractor never named the
      label in this run (even when the label has one known sense) and names none or
      several of the phrase's concepts under other labels, or the run's sense is a
      concept this tagging run had no candidate for (a sense split off since).
    A candidate for a concept other than the run's sense is dropped when that sense
    is among the phrase's candidates, so one sense is not spread to every course
    that uses the label. Ambiguity flags are review signals and decide nothing.
#}
-- A run tags only the sources whose text changed since the run before it, so the
-- newest run's source records name, per current source, the run whose mention rows
-- hold its mentions.
with tagged_sources as (
    select tagged_source.*
    from {{ source('courseware_intermediate', 'courseware_concept_tagging_source') }} as tagged_source
    inner join (select distinct tagging_run_id from {{ ref('int__courseware__concept_tagging_aliases') }}) as latest_run
        on tagged_source.tagging_run_id = latest_run.tagging_run_id
)

, mentions as (
    select mention.*
    from {{ source('courseware_intermediate', 'courseware_concept_mention') }} as mention
    inner join tagged_sources
        on
            mention.tagging_run_id = tagged_sources.mention_run_id
            and mention.platform = tagged_sources.platform
            and mention.courserun_readable_id = tagged_sources.courserun_readable_id
            and mention.scope_id = tagged_sources.scope_id
            and mention.source_ref = tagged_sources.source_ref
)

, aliases as (
    select * from {{ ref('int__courseware__concept_tagging_aliases') }}
)

, sense_assignments as (
    select * from {{ ref('int__courseware__concept_sense_assignments') }}
)

, run_concepts as (
    select distinct
        platform
        , courserun_readable_id
        , concept_id
    from sense_assignments
    where sense_status = 'assigned'
)

, phrase_concepts as (
    select distinct
        pattern_key
        , concept_id
    from aliases
)

-- how many of a phrase's candidate concepts each course run assigned under any label
, phrase_run_concepts as (
    select
        phrase_concepts.pattern_key
        , run_concepts.platform
        , run_concepts.courserun_readable_id
        , count(*) as assigned_candidate_count
    from phrase_concepts
    inner join run_concepts
        on phrase_concepts.concept_id = run_concepts.concept_id
    group by
        phrase_concepts.pattern_key
        , run_concepts.platform
        , run_concepts.courserun_readable_id
)

, redirected as (
    select
        mentions.tagging_run_id
        , mentions.platform
        , mentions.courserun_readable_id
        , mentions.scope_id
        , mentions.scope_kind
        , mentions.source_ref
        , mentions.source_kind
        , aliases.concept_id
        , aliases.tagged_concept_id
        , mentions.pattern_key
        , aliases.concept_label_key
        , aliases.variant
        , aliases.match_mode
        , aliases.is_short
        , aliases.is_qualified
        , aliases.is_single_word
        , aliases.has_multiple_concepts
        , aliases.is_ambiguous
        , case
            when sense_assignments.sense_status = 'needs_review' then 'needs_review'
            when sense_assignments.concept_id = aliases.concept_id then 'course_sense'
            -- another of the phrase's candidates is the run's sense of the label
            when sense_candidates.concept_id is not null then null
            -- the run never used the label, and named only this one of the phrase's
            -- concepts under another label; an explicit sense the dictionary has no
            -- candidate for, or several named concepts, decide nothing
            when
                sense_assignments.concept_label_key is null
                and run_concepts.concept_id is not null
                and phrase_run_concepts.assigned_candidate_count = 1
                then 'course_sense'
            else 'candidate'
        end as sense_status
        , mentions.matched_text
        , mentions.char_start
        , mentions.char_end
    from mentions
    inner join aliases
        on
            mentions.pattern_key = aliases.pattern_key
            and mentions.concept_id = aliases.tagged_concept_id
    left join sense_assignments
        on
            aliases.concept_label_key = sense_assignments.concept_label_key
            and mentions.platform = sense_assignments.platform
            and mentions.courserun_readable_id = sense_assignments.courserun_readable_id
    left join phrase_concepts as sense_candidates
        on
            mentions.pattern_key = sense_candidates.pattern_key
            and sense_assignments.concept_id = sense_candidates.concept_id
    left join run_concepts
        on
            mentions.platform = run_concepts.platform
            and mentions.courserun_readable_id = run_concepts.courserun_readable_id
            and aliases.concept_id = run_concepts.concept_id
    left join phrase_run_concepts
        on
            mentions.pattern_key = phrase_run_concepts.pattern_key
            and mentions.platform = phrase_run_concepts.platform
            and mentions.courserun_readable_id = phrase_run_concepts.courserun_readable_id
)

select
    tagging_run_id
    , platform
    , courserun_readable_id
    , scope_id
    , scope_kind
    , source_ref
    , source_kind
    , concept_id
    , tagged_concept_id
    , pattern_key
    , concept_label_key
    , variant
    , match_mode
    , is_short
    , is_qualified
    , is_single_word
    , has_multiple_concepts
    , is_ambiguous
    , sense_status
    , matched_text
    , char_start
    , char_end
from (
    select
        *
        , row_number() over (
            partition by platform, courserun_readable_id, source_ref, char_start, concept_id
            -- a resolved row wins over a candidate for the same span and concept
            order by
                case sense_status when 'course_sense' then 0 when 'needs_review' then 1 else 2 end
                , tagged_concept_id
        ) as span_rank
    from redirected
    -- the run uses the label in another sense
    where sense_status is not null
) as ranked
where span_rank = 1
