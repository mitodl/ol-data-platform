{% macro test_expect_no_value_overlap_across_rows(model, column_a, column_b) %}
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
    -- If `test_columns_no_value_overlap` is applied to `user_mitxonline_email` and `user_edxorg_email`,
    -- this row will be flagged
    --
    -- Output:
    -- - The query will return the rows that violate the no-overlap condition.

    select
        value
    from (
        select {{ column_a }} AS value
        from {{ ref(model_name) }}
        where {{ column_a }} IS NOT NULL

        union distinct

        select {{ column_b }} AS value
        from {{ ref(model_name) }}
        where {{ column_b }} IS NOT NULL
    ) t
    group by value
    having count(*) > 1



{% endmacro %}
