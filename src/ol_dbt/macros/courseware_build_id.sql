{#
    The dbt invocation building the model, as a string literal. The knowledge release
    candidate projections carry it so the release asset can tell whether the rows
    it reads are the build int__courseware__release_manifest read. A unit test
    overrides it to compare a fixed value.
#}
{% macro courseware_build_id() -%}
    '{{ invocation_id }}'
{%- endmacro %}
