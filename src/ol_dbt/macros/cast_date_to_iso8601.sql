{% macro cast_date_to_iso8601(column_name) %}
  case
    when try_cast({{ column_name }} AS date) is not null -- Use try_cast to check if the column is already a date
       then to_iso8601(try_cast({{ column_name }} AS date))

    else to_iso8601(from_iso8601_date(try_cast({{ column_name }} AS varchar))) -- Convert to ISO 8601 from a string
end
{% endmacro %}
