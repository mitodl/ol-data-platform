{#
    Shared by the OCW courseware models (int__courseware__ocw_files, _ocw_table_rows,
    _ocw_resources and int__courseware__extraction_sources), which each read a part of
    an OCW course's published site.

    courseware_ocw_course_filter: the OCW courses var('courseware_ocw_courses') selects,
    as a SQL list of slugs. Without it no OCW course is read: a deployed build sets no
    var, and int__courseware__extraction_allowlist (the courseware_extraction_allowlist
    seed) only approves mitxonline, mitxpro and residential runs.
#}
{% macro courseware_ocw_course_filter() -%}
    {%- set ocw_courses = var('courseware_ocw_courses', []) -%}
    {{ return("('" ~ ocw_courses | join("', '") ~ "')" if ocw_courses else "('')") }}
{%- endmacro %}


{# courseware_ocw_resource_href: a link to one of a course's resources, the resource's key as group 1 #}
{% macro courseware_ocw_resource_href() -%}
    {{ return("'href=\"/(courses/[^/\"]+/resources/[^/\"#?]+)'") }}
{%- endmacro %}


{# courseware_ocw_table_cell: one cell of a table row, its HTML as group 1 #}
{% macro courseware_ocw_table_cell() -%}
    {{ return("'(?s)<t[dh](?:\\s[^>]*)?>(.*?)</t[dh]>'") }}
{%- endmacro %}
