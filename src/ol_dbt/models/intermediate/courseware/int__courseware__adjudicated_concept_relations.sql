{#
    The concept relations resolution adjudication proposed (broader, close, related),
    from the newest run's snapshot, with when each was first proposed and the
    verdicts behind them.

    Verdicts come from the append-only decision log, mapped from labels onto
    concepts by the newest run's assignments, so a verdict about two labels counts
    for whichever concepts those labels now belong to. Only the newest verdict per
    (query label, candidate label) counts: a rebuild re-adjudicates every label, and
    counting its verdicts again would double the support with no new evidence. A
    concept pair is still often judged more than once (from both sides, and for each
    label of either concept).
      supporting_verdict_count  verdicts that agree with the relation: for broader,
                                those placing from under to; for close, close; for
                                related, related or either hierarchy direction (a
                                tied broader vote becomes related)
      contrary_verdict_count    distinct verdicts on the pair, plus, for broader,
                                those placing to under from
      pair_verdict_count        every verdict on the pair other than exact and
                                distinct, i.e. the verdicts saying the two concepts
                                are connected somehow; the same on each relation of
                                the pair
#}
with runs as (
    select * from {{ source('courseware_intermediate', 'courseware_resolution_run') }}
)

, latest_run as (
    select resolution_run_id
    from (
        select
            resolution_run_id
            , row_number() over (order by started_at desc) as run_rank
        from runs
    ) as ranked
    where run_rank = 1
)

, relations as (
    select relation.*
    from {{ source('courseware_intermediate', 'courseware_concept_relation') }} as relation
    inner join latest_run
        on relation.resolution_run_id = latest_run.resolution_run_id
)

, assignments as (
    select assignment.*
    from {{ source('courseware_intermediate', 'courseware_concept_assignment') }} as assignment
    inner join latest_run
        on assignment.resolution_run_id = latest_run.resolution_run_id
)

, latest_decisions as (
    select
        query_label_key
        , candidate_label_key
        , verdict
    from (
        select
            decision.query_label_key
            , decision.candidate_label_key
            , decision.verdict
            , row_number() over (
                partition by decision.query_label_key, decision.candidate_label_key
                order by runs.started_at desc, decision.verdict asc
            ) as decision_rank
        from {{ source('courseware_intermediate', 'courseware_resolution_decision') }} as decision
        inner join runs
            on decision.resolution_run_id = runs.resolution_run_id
    ) as ranked
    where decision_rank = 1
)

-- each verdict as (narrower, broader) for the hierarchy verdicts, query first otherwise
, verdicts as (
    select
        case when decision.verdict = 'broader' then candidate.concept_id else query.concept_id end as left_concept_id
        , case when decision.verdict = 'broader' then query.concept_id else candidate.concept_id end as right_concept_id
        , decision.verdict
    from latest_decisions as decision
    inner join assignments as query
        on decision.query_label_key = query.concept_label_key
    inner join assignments as candidate
        on decision.candidate_label_key = candidate.concept_label_key
    where query.concept_id != candidate.concept_id
)

, relation_verdicts as (
    select
        relations.from_concept_id
        , relations.to_concept_id
        , relations.relation
        , count(
            case
                when relations.relation = 'broader'
                    then
                        case
                            when
                                verdicts.verdict in ('broader', 'narrower')
                                and verdicts.left_concept_id = relations.from_concept_id
                                then 1
                        end
                when relations.relation = 'close' and verdicts.verdict = 'close' then 1
                when relations.relation = 'related' and verdicts.verdict in ('related', 'broader', 'narrower') then 1
            end
        ) as supporting_verdict_count
        , count(
            case
                when verdicts.verdict = 'distinct' then 1
                when
                    relations.relation = 'broader'
                    and verdicts.verdict in ('broader', 'narrower')
                    and verdicts.left_concept_id = relations.to_concept_id
                    then 1
            end
        ) as contrary_verdict_count
        , count(case when verdicts.verdict not in ('exact', 'distinct') then 1 end) as pair_verdict_count
    from relations
    left join verdicts
        on
            least(relations.from_concept_id, relations.to_concept_id)
            = least(verdicts.left_concept_id, verdicts.right_concept_id)
            and greatest(relations.from_concept_id, relations.to_concept_id)
            = greatest(verdicts.left_concept_id, verdicts.right_concept_id)
    group by relations.from_concept_id, relations.to_concept_id, relations.relation
)

select
    relations.from_concept_id
    , relations.to_concept_id
    , relations.relation
    , relations.proposed_run_id
    , runs.started_at as proposed_at
    , relation_verdicts.supporting_verdict_count
    , relation_verdicts.contrary_verdict_count
    , relation_verdicts.pair_verdict_count
from relations
inner join runs
    on relations.proposed_run_id = runs.resolution_run_id
inner join relation_verdicts
    on
        relations.from_concept_id = relation_verdicts.from_concept_id
        and relations.to_concept_id = relation_verdicts.to_concept_id
        and relations.relation = relation_verdicts.relation
