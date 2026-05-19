{{ config(
    materialized='table'
) }}

-- Maps MITx Online B2B organizations to course runs via contracts.
-- Grain: one row per (organization, contract, course_run).
-- Note: xPro B2B purchases are tracked via orders (see int__mitxpro__b2becommerce_b2border).
with contracts_to_courseruns as (
    select
        b2b_contract_id as contract_id
        , courserun_readable_id
    from {{ ref('stg__mitxonline__app__postgres__courses_courserun') }}
    where b2b_contract_id is not null
)

, dim_contract as (
    select contract_pk, contract_id, organization_fk
    from {{ ref('dim_contract') }}
)

, dim_course_run as (
    select courserun_pk, courserun_readable_id, platform
    from {{ ref('dim_course_run') }}
    where platform = 'mitxonline'
      and is_current = true
)

select
    dc.organization_fk
    , dc.contract_pk as contract_fk
    , cr.courserun_pk as courserun_fk
from contracts_to_courseruns as ctc
inner join dim_contract as dc on ctc.contract_id = dc.contract_id
inner join dim_course_run as cr on ctc.courserun_readable_id = cr.courserun_readable_id
