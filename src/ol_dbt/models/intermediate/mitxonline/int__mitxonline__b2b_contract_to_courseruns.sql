-- Grain: one row per (course run, contract). A run can be offered under several
-- contracts (CourseRun.b2b_contracts), possibly of different organizations.
with courseruns as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_courserun') }}
)

, courserun_contracts as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_courserun_b2b_contracts') }}
)

, b2b_contract as (
     select * from {{ ref('stg__mitxonline__app__postgres__b2b_contractpage') }}
)

, b2b_organization as (
    select * from {{ ref('stg__mitxonline__app__postgres__b2b_organizationpage') }}
)

select
    courseruns.courserun_readable_id
    , b2b_contract.b2b_contract_id
    , b2b_organization.organization_key
    , b2b_organization.organization_name
    , b2b_contract.b2b_contract_name
    , b2b_contract.b2b_contract_is_active
    , b2b_contract.b2b_contract_start_date
    , b2b_contract.b2b_contract_end_date
from courserun_contracts
inner join courseruns on courserun_contracts.courserun_id = courseruns.courserun_id
inner join b2b_contract on courserun_contracts.b2b_contract_id = b2b_contract.b2b_contract_id
inner join b2b_organization on b2b_contract.organization_id = b2b_organization.organization_id
