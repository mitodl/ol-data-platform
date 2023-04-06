with validation_errors as (

    select
        coursetopic_name
        , course_id
    from dev.main_intermediate.int__mitxonline__course_to_topics
    where
        1 = 1
        and
        not (
            coursetopic_name is null
            and course_id is null

        )



    group by
        coursetopic_name, course_id
    having count(*) > 1

)

select * from validation_errors
