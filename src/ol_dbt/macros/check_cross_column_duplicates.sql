{% macro test_check_cross_column_duplicates(model, column_a, column_b) %}
    {% set model_name = model if model is string else model.identifier %}

    -- This test checks whether a value from `column_a` occurs in `column_b`
    -- across different rows in the specified model. The test ensures mutual exclusivity.
    --
    -- Example:
    -- Given a table "int__mitx__users":
    -- | user_mitxonline_email   | user_edxorg_email |
    -- |-------------------------|-------------------|
    -- | alice@mail.com          | NULL             |
    -- | NULL                    | alice@mail.com   | <-- Violation: "alice@mail.com" exists in both columns across rows
    --
    --
    -- Output:
    -- - The query will return the rows that violate the no-overlap condition.

    select value
    from (
        select {{ column_a }} as value
        from {{ ref(model_name) }}
        where {{ column_a }} is not null
          and {{ column_b }} is null

        union all

        select {{ column_b }} as value
        from {{ ref(model_name) }}
        where {{ column_b }} is not null
          and {{ column_a }} is null
    ) combined
    group by value
    having count(*) > 1




{% endmacro %}
