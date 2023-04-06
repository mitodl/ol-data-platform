with validation_errors as (

    select
        line_id
        , courserun_id
    from dev.main_staging.stg__mitxpro__app__postgres__ecommerce_linerunselection
    where
        1 = 1
        and
        not (
            line_id is null
            and courserun_id is null

        )



    group by
        line_id, courserun_id
    having count(*) > 1

)

select * from validation_errors
