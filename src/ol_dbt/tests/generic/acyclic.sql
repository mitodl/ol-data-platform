{#
    Fails for each node on a directed cycle of at most max_length edges, so a
    relation meant as a strict partial order (e.g. learning order) can be held to it.

    A recursive CTE would be exact, but Trino's is experimental with a session-level
    depth cap and StarRocks needs a flag with a default depth of 5. This unrolls the
    walk instead: step k holds the distinct (start, end) pairs reachable in exactly k
    edges, and a start that has returned to itself is carried forward, so each step
    reads only the one before it and the edges. A cycle longer than max_length goes
    unseen here; ml.lib.courseware.graph finds every cycle with strongly connected
    components on each graph run, failing the run on an approved builds_on cycle and
    counting the other cycles.
#}
{% test acyclic(model, column_name, to_column, max_length=32) %}

{#- One node on a cycle breaks the order, so fail at one row, not the project's ten. -#}
{{ config(error_if='!=0') }}

with edges as (
    select distinct
        {{ column_name }} as from_id
        , {{ to_column }} as to_id
    from {{ model }}
    where {{ column_name }} is not null and {{ to_column }} is not null
)

, walk_1 as (
    select
        from_id as start_id
        , to_id as end_id
    from edges
)
{% for step in range(2, max_length + 1) %}
, walk_{{ step }} as (
    select distinct
        walk.start_id
        , coalesce(edges.to_id, walk.end_id) as end_id
    from walk_{{ step - 1 }} as walk
    left join edges
        on
            walk.end_id = edges.from_id
            and walk.start_id != walk.end_id
    where walk.start_id = walk.end_id or edges.to_id is not null
)
{% endfor %}
select start_id as {{ column_name }}
from walk_{{ max_length }}
where start_id = end_id

{% endtest %}
