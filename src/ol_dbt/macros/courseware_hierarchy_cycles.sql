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
    narrower one through path_edges ranked above it. Both relations have
    from_concept_id, to_concept_id and cycle_rank from courseware_hierarchy_edges.

    It walks from each start edge's broader concept, as the acyclic test does: a walk
    that is back at the edge's narrower concept is carried forward, every other walk
    is extended by one edge per step. A cycle longer than max_cycle_length goes unseen.
#}
{% macro courseware_hierarchy_cycle_cuts(cuts, start_edges, path_edges, max_cycle_length=32) -%}
, {{ cuts }}_walk_1 as (
    select
        from_concept_id as start_from_concept_id
        , to_concept_id as start_to_concept_id
        , cycle_rank as start_rank
        , to_concept_id as end_concept_id
    from {{ start_edges }}
)
{% for step in range(2, max_cycle_length + 1) %}
, {{ cuts }}_walk_{{ step }} as (
    select distinct
        walk.start_from_concept_id
        , walk.start_to_concept_id
        , walk.start_rank
        , coalesce(path_edges.to_concept_id, walk.end_concept_id) as end_concept_id
    from {{ cuts }}_walk_{{ step - 1 }} as walk
    left join {{ path_edges }} as path_edges
        on
            walk.end_concept_id = path_edges.from_concept_id
            and walk.end_concept_id != walk.start_from_concept_id
            and path_edges.cycle_rank > walk.start_rank
    where walk.end_concept_id = walk.start_from_concept_id or path_edges.to_concept_id is not null
)
{% endfor %}
, {{ cuts }} as (
    select distinct
        start_from_concept_id as from_concept_id
        , start_to_concept_id as to_concept_id
    from {{ cuts }}_walk_{{ max_cycle_length }}
    where end_concept_id = start_from_concept_id
)
{%- endmacro %}
