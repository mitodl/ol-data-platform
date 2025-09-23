with
    source as (select * from {{ source("ol_warehouse_raw_data", "raw__micromasters__app__postgres__exams_examrun") }}),
    renamed as (

        select
            id as examrun_id,
            course_id,
            exam_series_code,
            edx_exam_course_key as examrun_readable_id,
            semester as examrun_semester,
            description as examrun_description,
            authorized as examrun_is_authorized,
            passing_score as examrun_passing_grade,
            {{ cast_timestamp_to_iso8601("created_on") }} as examrun_created_on,
            {{ cast_timestamp_to_iso8601("updated_on") }} as examrun_updated_on
        from source

    )

select *
from renamed
