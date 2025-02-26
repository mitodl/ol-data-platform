{% macro cast_date_to_iso8601(column_name) %}
  case
   -- If the column is already a date, convert to ISO 8601
    when try_cast({{ column_name }} AS date) is not null
       then to_iso8601(try_cast({{ column_name }} AS date))
    -- otherwise, convert to ISO 8601 from a string
    else to_iso8601(from_iso8601_date(try_cast({{ column_name }} AS varchar)))
end
{% endmacro %}
