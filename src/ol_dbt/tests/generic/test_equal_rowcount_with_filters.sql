-- Modifies equal_rowcount from dbt_utils --
-- https://github.com/dbt-labs/dbt-utils/blob/7eab49274943964f46a5d2ddd326fac11a16598a/macros/generic_tests/equal_rowcount.sql --

{% test equal_rowcount_with_filters(model, compare_model, filter=null, compare_filter=null) %}

with a as (

    select count(*) as count_a from {{ model }}
    {% if filter %}
    where {{filter}}
    {% endif %}

),
b as (

    select count(*) as count_b from {{ compare_model }}
    {% if compare_filter %}
    where {{compare_filter}}
    {% endif %}

),
final as (

    select
        count_a,
        count_b,
        abs(count_a - count_b) as diff_count
    from a
    cross join b

)

select * from final
where diff_count > 0

{% endtest %}
