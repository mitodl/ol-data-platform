{#
    canvas_run_readable_id: the id of a Canvas course's one run in MIT Learn,
    <course_id>-<course_code>+canvas (run_for_canvas_archive in mit-learn). Learn
    formats a missing course code as "None", and so does the learn_key the canvas
    content source writes, which embeds this id.
#}
{% macro canvas_run_readable_id(course_id, course_code) -%}
    concat({{ course_id }}, '-', coalesce({{ course_code }}, 'None'), '+canvas')
{%- endmacro %}
