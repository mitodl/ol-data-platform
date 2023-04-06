select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (


    with test_data as (

        select
            cast('COURSERUN_READABLE_ID' as varchar) as column_name
            , 0 as matching_column_index
            , True as column_index_matches

    )

    select *
    from test_data
    where
        not(matching_column_index >= 0 and column_index_matches)

) as dbt_internal_test
