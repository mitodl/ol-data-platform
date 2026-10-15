-- Maps MITx Online B2B organizations to the course runs their contracts offer.
-- Grain: one row per (organization, contract, course_run). A run can be offered under
-- several contracts, so this is not the way to attribute an enrollment to a contract:
-- tfact_enrollment.contract_fk records the contract the learner enrolled under.
-- Note: xPro B2B purchases are tracked via orders (see int__mitxpro__b2becommerce_b2border).
with contracts_to_courseruns as (
    select
        courserun_contracts.b2b_contract_id as contract_id
        , courseruns.courserun_readable_id
    from {{ ref('stg__mitxonline__app__postgres__courses_courserun_b2b_contracts') }} as courserun_contracts
    inner join {{ ref('stg__mitxonline__app__postgres__courses_courserun') }} as courseruns
        on courserun_contracts.courserun_id = courseruns.courserun_id
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
