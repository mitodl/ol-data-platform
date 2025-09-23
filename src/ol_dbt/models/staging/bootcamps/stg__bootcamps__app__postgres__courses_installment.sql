with
    source as (
        select * from {{ source("ol_warehouse_raw_data", "raw__bootcamps__app__postgres__klasses_installment") }}
    ),
    cleaned as (

        select
            id as installment_id,
            bootcamp_run_id as courserun_id,
            cast(amount as decimal(38, 2)) as installment_amount,
            {{ cast_timestamp_to_iso8601("deadline") }} as installment_deadline
        from source
    )

select *
from cleaned
