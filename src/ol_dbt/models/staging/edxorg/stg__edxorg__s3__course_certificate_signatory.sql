with source as (
    select *
    from {{ source('ol_warehouse_raw_data', 'raw__edxorg__s3__course_structure__course_certificate_signatory') }}
)

, source_sorted as (
    select
        *
        , dense_rank() over (partition by course_id order by _ab_source_file_last_modified desc) as dense_row
    from source
)

, most_recent_source as (
    select * from source_sorted
    where dense_row = 1
)

, deduplicated as (
    select
        *
        , row_number() over (partition by id, course_id order by _airbyte_extracted_at desc) as row_num
    from most_recent_source
)

, final_source as (
    select * from deduplicated
    where row_num = 1
)

, cleaned as (

    select
        id as signatory_id
        , course_id as courserun_readable_id
        , title as signatory_title
        , organization as signatory_organization
        , certificate_id as signatory_certificate_id
        , trim(name) as signatory_name
        , regexp_replace(trim(name), '^(Professor |Dr\. )', '') as signatory_normalized_name
        , concat('https://courses.edx.org', nullif(signature_image_path, '')) as signatory_image_url
    from final_source
)

select * from cleaned
