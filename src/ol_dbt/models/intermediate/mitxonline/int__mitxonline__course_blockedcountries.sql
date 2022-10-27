-- Course's Blocked countries information for MITx Online
-- Keep it as separate model for flexibility to satisfy different use cases

with blockedcountries as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__courses_blockedcountry') }}
)

select
    course_id
    , blockedcountry_code
from blockedcountries
