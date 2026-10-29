{#
    Close relations that pass bridge_concept_relation's support rule, read both ways
    as (concept_fk, close_concept_fk), since bridge_concept_relation stores each close
    pair once. is_reviewed says whether the relation is accepted.
#}
{% macro courseware_close_concept_pairs() -%}
    select
        from_concept_fk as concept_fk
        , to_concept_fk as close_concept_fk
        , review_status = 'accepted' as is_reviewed
    from {{ ref('bridge_concept_relation') }}
    where relation = 'close' and passes_support_rule
    union
    select
        to_concept_fk as concept_fk
        , from_concept_fk as close_concept_fk
        , review_status = 'accepted' as is_reviewed
    from {{ ref('bridge_concept_relation') }}
    where relation = 'close' and passes_support_rule
{%- endmacro %}
