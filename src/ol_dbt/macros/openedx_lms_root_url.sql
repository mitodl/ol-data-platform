{#
  openedx_lms_root_url: the LMS root of an Open edX deployment, MIT Learn's
  CONTENT_BASE_URL_MITXONLINE or CONTENT_BASE_URL_XPRO in production. Null for
  any other source.
#}
{% macro openedx_lms_root_url(source_system) %}
    case {{ source_system }}
        when 'mitxonline' then 'https://courses.mitxonline.mit.edu'
        when 'xpro' then 'https://courses.xpro.mit.edu'
    end
{% endmacro %}
