with source as (
    select * from {{
     source('ol_warehouse_raw_data', 'raw__mitxonline__openedx__mysql__grades_persistentsubsectiongradeoverride')
   }}
)

{{ deduplicate_raw_table(order_by='modified' , partition_columns = 'grade_id') }}
, cleaned as (

    select
        id as subsectiongradeoverride_id
        , grade_id as subsectiongrade_id
        , possible_all_override as subsectiongradeoverride_total_score
        , possible_graded_override as subsectiongradeoverride_total_graded_score
        , earned_all_override as subsectiongradeoverride_total_earned_score
        , earned_graded_override as subsectiongradeoverride_total_earned_graded_score
        , override_reason as subsectiongradeoverride_reason
        , system as subsectiongradeoverride_system
        , to_iso8601(created) as subsectiongradeoverride_created_on
        , to_iso8601(modified) as subsectiongradeoverride_updated_on
    from most_recent_source
)

select * from cleaned
