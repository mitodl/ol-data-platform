with a as (

    select count(*) as expression

    from
        dev.main_intermediate.int__mitxonline__program_certificates


)

, b as (

    select count(*) * 1 as expression

    from
        dev.main_staging.stg__mitxonline__app__postgres__courses_programcertificate


)

, final as (

    select

        a.expression
        , b.expression as compare_expression
        , abs(coalesce(a.expression, 0) - coalesce(b.expression, 0)) as expression_difference
        , abs(coalesce(a.expression, 0) - coalesce(b.expression, 0))
        / nullif(a.expression * 1.0, 0) as expression_difference_percent
    from

        a
    cross join b

)

-- DEBUG:
-- select * from final
select *
from final
where

    expression_difference > 0.0
