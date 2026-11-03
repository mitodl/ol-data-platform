{#
    The description prompt version the courseware_description_approval seed
    approves, as a one-row select of prompt_version. A seed with more than one row,
    even of one version, approves nothing (null).
#}
{% macro courseware_approved_description_prompt() -%}
    select case when count(*) = 1 then max(prompt_version) end as prompt_version
    from {{ ref('courseware_description_approval') }}
{%- endmacro %}
