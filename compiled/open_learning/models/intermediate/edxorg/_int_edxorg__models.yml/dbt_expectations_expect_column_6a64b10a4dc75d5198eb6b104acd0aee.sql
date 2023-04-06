

    with test_data as (

        select
            cast('COURSEACTIVITIY_NUM_PROGRESS_CHECK' as TEXT) as column_name,
            -1 as matching_column_index,
            True as column_index_matches

    )
    select *
    from test_data
    where
        not(matching_column_index >= 0 and column_index_matches)
