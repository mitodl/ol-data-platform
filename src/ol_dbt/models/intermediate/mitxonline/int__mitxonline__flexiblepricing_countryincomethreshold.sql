with countryincomethreshold as (
    select *
    from
        {{ ref('stg__mitxonline__app__postgres__flexiblepricing_countryincomethreshold') }}
)

select
    countryincomethreshold_id
    , countryincomethreshold_created_on
    , countryincomethreshold_updated_on
    , countryincomethreshold_country_code
    , countryincomethreshold_income_threshold
from countryincomethreshold
