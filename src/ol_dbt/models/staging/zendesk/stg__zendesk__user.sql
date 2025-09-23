with
    source as (select * from {{ source("ol_warehouse_raw_data", "raw__thirdparty__zendesk_support__users") }})

    {{ deduplicate_raw_table(order_by="_airbyte_extracted_at", partition_columns="id") }},
    cleaned as (
        select
            id as user_id,
            url as user_api_url,
            name as user_name,
            alias as user_alias,
            email as user_email,
            phone as user_phone_number,
            photo as user_photo_object,
            active as user_is_active,
            permanently_deleted as user_is_permanently_deleted,
            suspended as user_is_suspended,
            shared as user_is_shared,
            verified as user_is_verified,
            details as user_details,
            notes as user_notes,
            chat_only as user_is_chat_only,
            moderator as user_is_moderator,
            restricted_agent as user_is_restricted_agent,
            signature as user_signature,
            locale as user_locale,
            locale_id as user_locale_id,
            time_zone as user_time_zone,
            iana_time_zone as user_iana_time_zone,
            user_fields,
            shared_agent as user_is_shared_agent,
            organization_id,
            default_group_id as user_default_group_id,
            ticket_restriction as user_ticket_restriction,
            only_private_comments as user_only_private_comments,
            two_factor_auth_enabled as user_has_two_factor_auth_enabled,
            nullif(tags, array[]) as user_tags,
            role as user_role,
            case
                when role_type = 0
                then 'custom agent'
                when role_type = 1
                then 'light agent'
                when role_type = 3
                then 'chat agent'
                when role_type = 4
                then 'admin'
                else cast(role_type as varchar)
            end as user_role_type,
            {{ cast_timestamp_to_iso8601("last_login_at") }} as user_last_login_at,
            {{ cast_timestamp_to_iso8601("created_at") }} as user_created_at,
            {{ cast_timestamp_to_iso8601("updated_at") }} as user_updated_at
        from most_recent_source
    )

select *
from cleaned
