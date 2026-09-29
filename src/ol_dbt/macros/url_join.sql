{#
  url_join: resolve `path` against a site root `base` (scheme and host, no trailing
  slash), the way Python's urljoin does for that case: an absolute URL is kept, a
  root-relative or relative path is appended, and a NULL path gives the base.
#}
{% macro url_join(base, path) -%}
    case
        when {{ path }} is null or {{ path }} = '' then {{ base }}
        when lower({{ path }}) like 'http://%' or lower({{ path }}) like 'https://%' then {{ path }}
        when {{ path }} like '/%' then concat({{ base }}, {{ path }})
        else concat({{ base }}, '/', {{ path }})
    end
{%- endmacro %}
