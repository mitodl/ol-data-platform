-- int_finance__joined_ol_enrollments.sql

with mitxonline as (

    select *, Null as xpro_program_active, 'mitxonline' as org 
    from {{ ref('stg_mitxonline__app__postgres__course_courserunenrollment') }}

),

xpro as (

    select cre.*, pe.active as xpro_program_active, 'xpro' as org 
    from {{ ref('stg_mitxpro__app__postgres__courses_courserunenrollment') }} cre
    join {{ ref('stg_mitxpro__app__postgres__courses_programenrollment') }} pe on pe.user_id=cre.user_id
),

join_ol_enrollments as (

    select * from mitxonline

    union all

    select * from xpro

)

select 
    *, 
    extract(year from date_add('month', 6, cast(created_on as date))) as fiscal_year
from join_ol_enrollments
