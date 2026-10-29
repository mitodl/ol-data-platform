{# courseware_verdict_reliability: the chance one adjudication verdict is right #}
{% macro courseware_verdict_reliability() -%}
    0.5
{%- endmacro %}


{#
    courseware_hierarchy_edges: broader edges in cycle-breaking order, lowest first.
    edges is a relation of adjudicated broader edges with from_concept_id,
    to_concept_id, verdict_support (at least 1) and contrary_verdict_support. Edges are
    ranked by adjudication score (support_score without the cross-signal term),
    then by how many broader edges point at the broader concept, then by concept ids,
    so the order is total.
#}
{% macro courseware_hierarchy_edges(edges) -%}
    select
        from_concept_id
        , to_concept_id
        , row_number() over (
            order by adjudication_score, to_narrower_count, from_concept_id, to_concept_id
        ) as cycle_rank
    from (
        select
            from_concept_id
            , to_concept_id
            , round(
                cast(verdict_support as double) / (verdict_support + contrary_verdict_support)
                * (1.0 - power(1.0 - {{ courseware_verdict_reliability() }}, verdict_support))
                , 4
            ) as adjudication_score
            , count(*) over (partition by to_concept_id) as to_narrower_count
        from {{ edges }} as broader_edges
    ) as ranked
{%- endmacro %}


{#
    courseware_hierarchy_cycle_cuts: CTEs, each led by a comma, ending in one named
    cuts that holds every edge of start_edges whose broader concept leads back to its
    narrower one through path_edges ranked above it, or may, with cut_reason. Both
    relations have from_concept_id, to_concept_id and cycle_rank from
    courseware_hierarchy_edges.

    From each start edge's broader concept it walks path_edges ranked above the start
    edge, one edge per step, keeping every (concept, step) reached. Each step reads
    only the one before it, once, joined to the edges plus one stay row per concept
    that carries what was reached forward, because engines that inline CTEs (Trino)
    would repeat the walk per reference, and an aggregate per step makes DuckDB's
    planner time grow exponentially with the steps. A concept's first step is taken
    once, at the end.
      cycle       the walk reached the start edge's narrower concept within
                  max_cycle_length edges, the start edge included
      unresolved  it did not, but a concept first reached at the last step has an
                  edge onward, so a longer cycle cannot be ruled out. The edge is cut
                  anyway (fail closed), so the edges left are acyclic whatever the
                  length of a cycle; the cost is cutting an edge beneath a chain of
                  more than max_cycle_length broader edges.
#}
{% macro courseware_hierarchy_cycle_cuts(cuts, start_edges, path_edges, max_cycle_length=32) -%}
{#- a rank above every edge's, so a stay row joins whatever the threshold -#}
{%- set stay_rank = 9223372036854775807 -%}
, {{ cuts }}_moves as (
    select
        from_concept_id
        , to_concept_id
        , cycle_rank
        , false as is_stay
    from {{ path_edges }}
    union all
    select distinct
        concept_id as from_concept_id
        , concept_id as to_concept_id
        , {{ stay_rank }} as cycle_rank
        , true as is_stay
    from (
        select to_concept_id as concept_id from {{ start_edges }}
        union all
        select to_concept_id as concept_id from {{ path_edges }}
    ) as concepts
)

, {{ cuts }}_reach_1 as (
    select
        from_concept_id as start_from_concept_id
        , to_concept_id as start_to_concept_id
        , cycle_rank as start_rank
        , to_concept_id as concept_id
        , 1 as step
    from {{ start_edges }}
)
{% for step in range(2, max_cycle_length + 1) %}
, {{ cuts }}_reach_{{ step }} as (
    select distinct
        reach.start_from_concept_id
        , reach.start_to_concept_id
        , reach.start_rank
        , moves.to_concept_id as concept_id
        , case when moves.is_stay then reach.step else {{ step }} end as step
    from {{ cuts }}_reach_{{ step - 1 }} as reach
    -- every row stays; a row reached at the step before, short of the start edge's
    -- narrower concept, also moves along an edge ranked above the start edge
    inner join {{ cuts }}_moves as moves
        on
            reach.concept_id = moves.from_concept_id
            and moves.cycle_rank > case
                when reach.step = {{ step - 1 }} and reach.concept_id != reach.start_from_concept_id
                    then reach.start_rank
                else {{ stay_rank - 1 }}
            end
)
{% endfor %}
, {{ cuts }} as (
    select
        reach.start_from_concept_id as from_concept_id
        , reach.start_to_concept_id as to_concept_id
        , case
            when max(case when reach.concept_id = reach.start_from_concept_id then 1 else 0 end) = 1
                then 'cycle'
            else 'unresolved'
        end as cut_reason
    from (
        select
            start_from_concept_id
            , start_to_concept_id
            , start_rank
            , concept_id
            , min(step) as first_step
        from {{ cuts }}_reach_{{ max_cycle_length }}
        group by start_from_concept_id, start_to_concept_id, start_rank, concept_id
    ) as reach
    left join {{ path_edges }} as onward_edges
        on
            reach.first_step = {{ max_cycle_length }}
            and reach.concept_id != reach.start_from_concept_id
            and reach.concept_id = onward_edges.from_concept_id
            and onward_edges.cycle_rank > reach.start_rank
    group by reach.start_from_concept_id, reach.start_to_concept_id
    having
        max(case when reach.concept_id = reach.start_from_concept_id then 1 else 0 end) = 1
        or count(onward_edges.to_concept_id) > 0
)
{%- endmacro %}
