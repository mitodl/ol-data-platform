{#
    Published close relations read both ways, (concept_fk, close_concept_fk), since
    bridge_concept_relation stores each close pair once.
#}
{% macro courseware_close_concept_pairs() -%}
    select
        from_concept_fk as concept_fk
        , to_concept_fk as close_concept_fk
    from {{ ref('bridge_concept_relation') }}
    where relation = 'close' and is_published
    union
    select
        to_concept_fk as concept_fk
        , from_concept_fk as close_concept_fk
    from {{ ref('bridge_concept_relation') }}
    where relation = 'close' and is_published
{%- endmacro %}
