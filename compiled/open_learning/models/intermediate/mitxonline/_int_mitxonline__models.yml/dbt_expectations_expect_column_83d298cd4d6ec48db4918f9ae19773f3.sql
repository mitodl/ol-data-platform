with grouped_expression as (
    select program_id is null as expression





    from dev.main_intermediate.int__mitxonline__flexiblepricing_flexiblepriceapplication
    where
        courseware_type = 'course'



)

, validation_errors as (

    select *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors
