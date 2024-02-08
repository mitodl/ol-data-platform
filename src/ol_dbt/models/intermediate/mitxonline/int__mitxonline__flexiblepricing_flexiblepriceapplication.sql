with flexiblepriceapplication as (
    select * from {{ ref('stg__mitxonline__app__postgres__flexiblepricing_flexiblepriceapplication') }}
)

select *
from flexiblepriceapplication
