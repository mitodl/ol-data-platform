with source as (
    select *
    from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__openedx__mysql__grades_persistentsubsectiongrade') }}
)


{{ deduplicate_data('source', 'most_recent_source') }}

, cleaned as (

    select
        id as subsectiongrade_id
        , course_id as courserun_readable_id
        , user_id as openedx_user_id
        , usage_key as coursestructure_block_id
        , visible_blocks_hash as visibleblocks_hash
        , possible_all as subsectiongrade_total_score
        , possible_graded as subsectiongrade_total_graded_score
        , earned_all as subsectiongrade_total_earned_score
        , earned_graded as subsectiongrade_total_earned_graded_score
        , to_iso8601(from_iso8601_timestamp_nanos(first_attempted)) as subsectiongrade_first_attempted_on
        , to_iso8601(from_iso8601_timestamp_nanos(created)) as subsectiongrade_created_on
        , to_iso8601(from_iso8601_timestamp_nanos(modified)) as subsectiongrade_updated_on
    from most_recent_source
)

select * from cleaned
