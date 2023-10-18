{{ config(materialized='view') }}

with mitx_certificates as (
    --revoked certificates are already filtered
    select * from {{ ref('int__mitx__courserun_certificates') }}
)

, mitxpro_certificates as (
    select * from {{ ref('int__mitxpro__courserun_certificates') }}
    where courseruncertificate_is_revoked = false
)

, bootcamps_certificates as (
    select * from {{ ref('int__bootcamps__courserun_certificates') }}
    where courseruncertificate_is_revoked = false
)

select
    platform
    , courseruncertificate_url
    , courseruncertificate_created_on
    , user_id
    , courserun_id
    , courserun_title
    , courserun_readable_id
    , if(platform = '{{ var("mitxonline") }}', user_mitxonline_username, user_edxorg_username) as user_username
    , user_email
    , user_full_name
from mitx_certificates

union all

select
    '{{ var("mitxpro") }}' as platform
    , courseruncertificate_url
    , courseruncertificate_created_on
    , user_id
    , courserun_id
    , courserun_title
    , courserun_readable_id
    , user_username
    , user_email
    , user_full_name
from mitxpro_certificates

union all

select
    '{{ var("bootcamps") }}' as platform
    , courseruncertificate_url
    , courseruncertificate_created_on
    , user_id
    , courserun_id
    , courserun_title
    , courserun_readable_id
    , user_username
    , user_email
    , user_full_name
from bootcamps_certificates
