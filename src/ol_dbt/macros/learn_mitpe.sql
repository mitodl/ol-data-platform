{% macro learn_mitpe_location(format_expr, location_expr) %}
  {#
    MIT PE location as MIT Learn's mitpe ETL wrote it (parse_location in
    learning_resources/etl/mitpe.py): the feed's location for on-site formats, else
    the empty string, though the feed gives online items a location too.
  #}
    case
        when {{ format_expr }} in ('In Person', 'On Campus', 'Blended') then coalesce({{ location_expr }}, '')
        else ''
    end
{% endmacro %}

{% macro learn_mitpe_price(price_text_expr) %}
  {#
    MIT PE price as MIT Learn's mitpe ETL computed it: every character but digits and
    dots stripped, so a range's digits run together ("$1,500-$2,000" is 15002000).
    int__mitpe__learning_resources.price is null for a range instead.
  #}
    {{ try_cast(regexp_replace_all(price_text_expr, "'[^0-9.]'", "''"), 'decimal(12, 2)') }}
{% endmacro %}
