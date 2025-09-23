-- DEDP Course Certificate Information from MicroMaster DB
with
    source as (
        select *
        from
            {{
                source(
                    "ol_warehouse_raw_data", "raw__micromasters__app__postgres__grades_micromasterscoursecertificate"
                )
            }}
    ),
    cleaned as (
        select
            id as coursecertificate_id,
            user_id,
            course_id,
            hash as coursecertificate_hash,
            concat('https://micromasters.mit.edu/certificate/course/', hash) as coursecertificate_url,
            {{ cast_timestamp_to_iso8601("created_on") }} as coursecertificate_created_on,
            {{ cast_timestamp_to_iso8601("updated_on") }} as coursecertificate_updated_on

        from source
    )

select *
from cleaned
