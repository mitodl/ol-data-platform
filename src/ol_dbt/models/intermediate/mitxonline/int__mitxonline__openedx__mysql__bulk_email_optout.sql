-- MITx Online Openedx mySQL Bulk Email Optout

with bulk_email_optout as (
    select *
    from {{ ref('stg__mitxonline__openedx__mysql__bulk_email_optout') }}
)

select *
from bulk_email_optout
