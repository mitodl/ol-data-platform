{{
    config(
        materialized="table",
        tags=["b2b_analytics", "smoke_test"],
    )
}}

select 1 as smoke_test_value, now() as run_at
