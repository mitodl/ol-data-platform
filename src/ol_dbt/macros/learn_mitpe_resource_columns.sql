{% macro learn_mitpe_resource_columns(alias) %}
  {#
    The int__mitpe__learning_resources columns MIT Learn's webhook takes for a course or
    a program, shaped as MIT Learn's legacy mitpe ETL wrote them
    (learning_resources/etl/mitpe.py): a delivery code with unknown formats as online,
    the location only for on-site formats, empty strings for missing text, the
    duration in weeks by Learn's parse_resource_duration (which reads hours as weeks),
    and the price as the price text's digits and dots.
    The intermediate keeps the feed's labels and a correct duration parse.
  #}
    {{ alias }}.readable_id
    , {{ alias }}.title
    , {{ alias }}.url
    , {{ alias }}.image_url
    , {{ alias }}.image_alt
    , {{ alias }}.description
    , {{ alias }}.topics
    , {{ learn_delivery(alias ~ '.learning_format') }} as delivery
    , case
        when {{ alias }}.learning_format in ('In Person', 'On Campus', 'Blended')
            then coalesce({{ alias }}.location, '')
        else ''
    end as location
    , coalesce({{ alias }}.duration, '') as duration
    , {{ learn_duration_weeks(alias ~ '.duration', 'min') }} as min_weeks
    , {{ learn_duration_weeks(alias ~ '.duration', 'max') }} as max_weeks
    -- Learn strips every character but digits and dots, so a range's digits run
    -- together; the intermediate's price is null for one
    , {{ try_cast(regexp_replace_all(alias ~ '.price_text', "'[^0-9.]'", "''"), 'decimal(12, 2)') }} as price
    , {{ alias }}.instructors
{% endmacro %}
