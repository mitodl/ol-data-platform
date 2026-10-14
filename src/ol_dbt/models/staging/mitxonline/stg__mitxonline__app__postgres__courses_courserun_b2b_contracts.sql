with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__courses_courserun_b2b_contracts') }}
)

{{ deduplicate_raw_table(raw_table='raw__mitxonline__app__postgres__courses_courserun_b2b_contracts', partition_columns='id') }}

select
    id as courserun_b2b_contract_id
    , courserun_id
    , contractpage_id as b2b_contract_id
from most_recent_source
