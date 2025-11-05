with courseruns as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_courserun') }}
)

, b2b_contract as (
     select * from {{ ref('stg__mitxonline__app__postgres__b2b_contractpage') }}
)

, b2b_organization as (
    select * from {{ ref('stg__mitxonline__app__postgres__b2b_organizationpage') }}
)

select
    courseruns.courserun_readable_id
    , b2b_organization.organization_key
    , b2b_organization.organization_name
    , b2b_contract.b2b_contract_name
    , b2b_contract.b2b_contract_is_active
    , b2b_contract.b2b_contract_start_date
    , b2b_contract.b2b_contract_end_date
from b2b_contract
inner join b2b_organization on b2b_contract.organization_id = b2b_organization.organization_id
inner join courseruns on b2b_contract.b2b_contract_id = courseruns.b2b_contract_id
