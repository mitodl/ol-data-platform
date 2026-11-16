-- MITx Online Course Department Information

with source as (
    {{ live_raw_rows('raw__mitxonline__app__postgres__courses_department') }}
)

{{ deduplicate_raw_table(raw_table='raw__mitxonline__app__postgres__courses_department', partition_columns='id') }}

, cleaned as (
    select
        id as coursedepartment_id
        , name as coursedepartment_name
        ,{{ cast_timestamp_to_iso8601('created_on') }} as coursedepartment_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as coursedepartment_updated_on
    from most_recent_source
)

select * from cleaned
