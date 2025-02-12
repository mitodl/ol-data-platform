{% macro cast_timestamp_to_iso8601(column_name) %}
  case
       -- If the column is already a timestamp, convert to ISO 8601
        when try_cast({{ column_name }} AS timestamp) is not null
           then to_iso8601(try_cast({{ column_name }} AS timestamp))
        -- otherwise, convert to ISO 8601 from a string
        else to_iso8601(from_iso8601_timestamp(try_cast({{ column_name }} AS varchar)))
   end
{% endmacro %}
