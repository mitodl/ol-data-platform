with source as (
    select * from dev.main_staging.stg__mitxonline__app__postgres__flexiblepricing_flexiblepricetier
)

select
    flexiblepricetier_id
    , flexiblepricetier_is_current
    , flexiblepricetier_created_on
    , flexiblepricetier_updated_on
    , discount_id
    , courseware_object_id
    , flexiblepricetier_income_threshold_usd
    , contenttype_id

from source
