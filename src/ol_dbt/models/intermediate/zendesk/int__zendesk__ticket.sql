with ticket as (  -- noqa: PRS
   select * from {{ ref('stg__zendesk__ticket') }}
)

, user as (
    select * from {{ ref('stg__zendesk__user') }}
)

, organization as (
    select * from {{ ref('stg__zendesk__organization') }}
)

, brand as (
    select * from {{ ref('stg__zendesk__brand') }}
)

, field as (
    select * from {{ ref('stg__zendesk__ticket_field') }}
)

, groups as (
    select * from {{ ref('stg__zendesk__group') }}
)

, named_custom_fields as (
    SELECT
      ticket.ticket_id
      , json_format(
         cast(
          array_agg(
            json_parse(
              json_object(
                'name' : field.field_title,
                'value' : value
              )
            )
          ) as json
        )
      ) as custom_fields
    from ticket
    cross join unnest(ticket.ticket_custom_fields) AS t(json_str)
    cross join unnest(
      array[
        cast(json_parse(json_str) AS row(id bigint, value varchar))
      ]
    ) as u(id, value)
    join field on id = field.field_id
    where value is not null
    group by ticket.ticket_id
)


select
    ticket.ticket_id
    , ticket.ticket_api_url
    , ticket.ticket_type
    , ticket.ticket_tags
    , ticket.ticket_subject
    , ticket.ticket_description
    , ticket.ticket_priority
    , ticket.ticket_status
    , ticket.ticket_source_channel
    , ticket.ticket_source_email
    , ticket.ticket_source_rel
    , requester.user_name as ticket_requester
    , assignee.user_name as ticket_assignee
    , submitter.user_name as ticket_submitter
    , ticket.ticket_recipient_email
    , ticket.ticket_email_cc_user_ids
    , ticket.ticket_follower_user_ids
    , ticket.ticket_collaborator_user_ids
    , brand.brand_name
    , groups.group_name
    , ticket.organization_id
    , organization.organization_name
    , named_custom_fields.custom_fields
    , ticket.ticket_due_at
    , ticket.ticket_has_incidents
    , ticket.ticket_is_public
    , ticket.ticket_satisfaction_rating_score
    , ticket.ticket_satisfaction_rating_comment
    , ticket.ticket_satisfaction_rating_reason
    , ticket.ticket_satisfaction_rating_object
    , ticket.ticket_unix_timestamp
    , ticket.ticket_created_at
    , ticket.ticket_updated_at
from ticket
left join organization
    on ticket.organization_id = organization.organization_id
left join brand
    on ticket.brand_id = brand.brand_id
left join groups
    on ticket.group_id = groups.group_id
left join user as submitter
    on ticket.ticket_submitter_user_id = submitter.user_id
left join user as requester
    on ticket.ticket_requester_user_id = requester.user_id
left join user as assignee
    on ticket.ticket_assignee_user_id = assignee.user_id
left join named_custom_fields
    on ticket.ticket_id = named_custom_fields.ticket_id
