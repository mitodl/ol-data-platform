-- MITx Online Course Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__courses_course') }}
)

{{ deduplicate_raw_table(raw_table='raw__mitxonline__app__postgres__courses_course', partition_columns='id') }}

, cleaned as (
    select
        id as course_id
        , live as course_is_live
        , title as course_title
        , readable_id as course_readable_id
        , replace(replace(readable_id, 'course-v1:', ''), '+', '/') as course_edx_readable_id
        -- QA's test courses have ids with no '+' (e.g. course-16). Those have no
        -- course number of their own, so the whole id stands in for it.
        , coalesce({{ element_at_array("split(readable_id, '+')", 2) }}, readable_id) as course_number
        ,{{ cast_timestamp_to_iso8601('created_on') }} as course_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as course_updated_on
    from most_recent_source
)

select * from cleaned
