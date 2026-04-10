{{ config(
    materialized='table'
) }}

with xpro_tax_rates as (
    select * from {{ ref('stg__mitxpro__app__postgres__ecommerce_taxrate') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['taxrate_country_code']) }} as tax_rate_pk
    , taxrate_country_code as country_code
    , taxrate_tax_rate as tax_rate
    , taxrate_tax_rate_name as tax_rate_name
    , taxrate_is_active as is_active
from xpro_tax_rates
