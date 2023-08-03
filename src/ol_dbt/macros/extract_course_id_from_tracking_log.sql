{% macro extract_course_id_from_tracking_log() %}
    ---course ID format: course-v1:{org}+{course number}+{run_tag}
    {% set course_id_regex = 'course-v(\d{1}):([^\/]+)\+([^\/]+)\+([^\/\]]+)' %}
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
