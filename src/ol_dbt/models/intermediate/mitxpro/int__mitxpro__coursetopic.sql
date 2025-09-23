with coursetopic as (select * from {{ ref("stg__mitxpro__app__postgres__courses_coursetopic") }})

select
    coursetopic_id, coursetopic_name, coursetopic_parent_coursetopic_id, coursetopic_created_on, coursetopic_updated_on
from coursetopic
