{% macro extract_course_id_from_tracking_log(course_id_has_old_format=false) %}
    ---course ID format: course-v1:{org}+{course number}+{run_tag} for courses created since Fall 2014
    ---course ID format: {org}/{course}/{run} for courses created before Fall 2014
    ---Course number and run tag can be letters, numbers, period, dashes, underscores
    {% if course_id_has_old_format %}
    {% set course_id_regex = '(course-v(\d{1}):([\w\.\-\_]+)\+([\w\.\-\_]+)\+([\w\.\-\_]+))|(([\w\.\-\_]+)\/([\w\.\-\_]+)\/([\w\.\-\_]+))'%}
    {% else %}
    {% set course_id_regex = 'course-v(\d{1}):([\w\.\-\_]+)\+([\w\.\-\_]+)\+([\w\.\-\_]+)' %}
{% endif %}

      case
          when regexp_extract(json_query(context, 'lax $.course_id' omit quotes), '{{ course_id_regex }}') is not null
             then json_query(context, 'lax $.course_id' omit quotes)
          when regexp_extract(json_query(context, 'lax $.path' omit quotes), '{{ course_id_regex }}') is not null
              then regexp_extract(json_query(context, 'lax $.path' omit quotes), '{{ course_id_regex }}')
          when regexp_extract(event_type, '{{ course_id_regex }}') is not null
              then regexp_extract(event_type, '{{ course_id_regex }}')
          when regexp_extract(page, '{{ course_id_regex }}') is not null
              then regexp_extract(page, '{{ course_id_regex }}')
      end
{% endmacro %}
