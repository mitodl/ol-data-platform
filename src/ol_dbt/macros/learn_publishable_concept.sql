{#
    learn_publishable_concept: the dim_concept rows Learn may show, as a predicate on
    dim_concept's columns. Every Learn view filters concepts with it, so a course
    never lists a concept the concepts view leaves out.
#}
{% macro learn_publishable_concept() -%}
    review_status = 'accepted'
    and lifecycle_status = 'active'
    and coalesce(is_learner_facing, false)
    -- a concept whose labels no current claim names stays active until a rebuild
    and label_count > 0
{%- endmacro %}
