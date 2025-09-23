-- MITxPro Program Run Information
with
    source as (select * from {{ source("ol_warehouse_raw_data", "raw__xpro__app__postgres__courses_programrun") }}),
    cleaned as (
        select
            id as programrun_id,
            program_id,
            run_tag as programrun_tag,
            {{ cast_timestamp_to_iso8601("start_date") }} as programrun_start_on,
            {{ cast_timestamp_to_iso8601("end_date") }} as programrun_end_on,
            {{ cast_timestamp_to_iso8601("created_on") }} as programrun_created_on,
            {{ cast_timestamp_to_iso8601("updated_on") }} as programrun_updated_on
        from source
    )

select *
from cleaned
