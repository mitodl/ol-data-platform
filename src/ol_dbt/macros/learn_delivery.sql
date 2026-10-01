{% macro learn_delivery(delivery_expr) %}
  {#
    MIT Learn delivery code for a source's delivery label, as MIT Learn's
    transform_delivery mapped it (RESOURCE_DELIVERY_MAPPING in
    learning_resources/etl/constants.py). Anything unrecognized, including null and
    the empty string, is online.
  #}
  case {{ delivery_expr }}
      when 'Blended' then 'hybrid'
      when 'Hybrid' then 'hybrid'
      when 'In Person' then 'in_person'
      when 'In person' then 'in_person'
      when 'On Campus' then 'in_person'
      when 'Offline' then 'offline'
      else 'online'
  end
{% endmacro %}
