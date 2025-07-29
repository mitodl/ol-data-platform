with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__thirdparty__zendesk_support__tickets') }}
)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at' , partition_columns = 'id') }}
, cleaned as (
    select
        id as ticket_id
        , via as ticket_via_object
        , url as ticket_api_url
        , tags as ticket_tags
        , type as ticket_type
        , status as ticket_status
        , description as ticket_description
        , subject as ticket_subject
        , raw_subject as ticket_raw_subject
        , brand_id
        , group_id
        , priority as ticket_priority
        , is_public as ticket_is_public
        , recipient as ticket_recipient_email
        , problem_id
        , requester_id as ticket_requester_user_id
        , submitter_id as ticket_submitter_user_id
        , assignee_id as ticket_assignee_user_id
        , email_cc_ids as ticket_email_cc_user_ids
        , follower_ids as ticket_follower_user_ids
        , collaborator_ids as ticket_collaborator_user_ids
        , forum_topic_id
        , ticket_form_id
        , organization_id
        , custom_status_id
        , sharing_agreement_ids
        , allow_attachments as ticket_allow_attachments
        , allow_channelback as ticket_allow_channelback
        , has_incidents as ticket_has_incidents
        , satisfaction_rating as ticket_satisfaction_rating_object
        , from_messaging_channel as ticket_is_from_messaging_channel
        , due_at as ticket_due_at
        , custom_fields as ticket_custom_fields
        , generated_timestamp as ticket_unix_timestamp
        , json_query(satisfaction_rating, 'lax $.score' omit quotes) as ticket_satisfaction_rating_score
        , json_query(satisfaction_rating, 'lax $.comment' omit quotes) as ticket_satisfaction_rating_comment
        , json_query(satisfaction_rating, 'lax $.reason' omit quotes) as ticket_satisfaction_rating_reason
        , json_query(via, 'lax $.channel' omit quotes) as ticket_source_channel
        , json_query(via, 'lax $.source.from.address' omit quotes) as ticket_source_email
        , json_query(via, 'lax $.source.from.ticket_id' omit quotes) as ticket_source_ticket_id
        , json_query(via, 'lax $.source.rel' omit quotes) as ticket_source_rel
        , {{ cast_timestamp_to_iso8601('created_at') }} as ticket_created_at
        , {{ cast_timestamp_to_iso8601('updated_at') }} as ticket_updated_at
    from most_recent_source
)

select * from cleaned
