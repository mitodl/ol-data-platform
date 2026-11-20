-- ad_opt_v2's parse_change_history inputs: budget, status, schedule and keyword changes
-- from the report, then the JSONL after the report ends, then the API after the JSONL ends.
{%- set budget_re = "'^(.+?): Budget amount (?:changed from \\$([0-9,.]+) to|is) \\$([0-9,.]+)$'" %}
{%- set campaign_header_re = "'^(?:[0-9]+ campaigns? [a-z]+|Campaign (?:created|changed))$'" %}
{%- set status_re = "'^Status (?:changed from ([A-Za-z]+) to|is) ([A-Za-z]+)$'" %}
{%- set schedule_re = "'^(Start|End) time (?:changed from (.+?) to|is) (.+)$'" %}
{%- set keyword_header_re = "'^[0-9]+ (negative )?(broad|phrase|exact) match keywords? (added|removed|paused|enabled)$'" %}
{%- set ad_group_header_re = "'^(?:[0-9]+ ad groups? (?:paused|enabled|removed)|Ad group created)$'" %}
{%- set keyword_detail_re = "'^-?(?:\\[([^\\]]+)\\]|\"([^\"]+)\"|([^:]+))'" %}
{%- set schedule_date_re = "'[A-Z][a-z]{2} [0-9]{1,2}, [0-9]{4}'" %}

-- history_folder is ad_opt_v2's course folder its history files came from; the API takes over
-- after a folder's history ends, in that folder's account. Each change's course comes from its
-- campaign at the end.
with history_accounts as (
    select history_folder, account_id from {{ ref('google_ads_history_accounts') }}
)

, report_lines as (
    select
        report.change_course as history_folder
        , report.change_row_number as row_key
        , report.change_at
        , trim(report.campaign_name) as campaign_name
        , trim(report.ad_group_name) as ad_group_name
        , lines.line_number
        , {{ element_at_array(regexp_split('report.change_text', "'\\r?\\n'"), 'lines.line_number') }} as line
    from {{ ref('stg__google_ads__s3__change_history_report') }} as report
    cross join
        {{ unnest_sequence(array_length(regexp_split('report.change_text', "'\\r?\\n'")), 'lines', 'line_number') }}
)

-- A header line is unindented; the indented lines under it are its details.
, report_grouped as (
    select
        history_folder
        , row_key
        , change_at
        , campaign_name
        , ad_group_name
        , line
        , line_number
        , sum(case when line not like ' %' and trim(line) != '' then 1 else 0 end) over (
            partition by history_folder, row_key order by line_number nulls last rows unbounded preceding
        ) as group_number
    from report_lines
)

, report_details as (
    select
        details.history_folder
        , 'report' as change_source
        , details.change_at
        , details.campaign_name
        , details.ad_group_name
        , trim(headers.line) as change_header
        , trim(details.line) as change_detail
        -- The report lists rows newest first; same-second changes apply bottom row first.
        , -details.row_key as sort_1
        , details.group_number as sort_2
        , details.line_number as sort_3
    from report_grouped as details
    inner join report_grouped as headers
        on
            details.history_folder = headers.history_folder
            and details.row_key = headers.row_key
            and details.group_number = headers.group_number
            and headers.line not like ' %'
    where details.line like ' %' and trim(details.line) != ''
)

, report_ends as (
    select change_course as history_folder, max(change_at) as last_change_at
    from {{ ref('stg__google_ads__s3__change_history_report') }}
    group by change_course
)

, jsonl_details as (
    select
        jsonl.change_course as history_folder
        , 'jsonl' as change_source
        , jsonl.change_at
        , trim(jsonl.campaign_name) as campaign_name
        , trim(jsonl.ad_group_name) as ad_group_name
        , trim(jsonl.change_header) as change_header
        , trim({{ element_at_array(json_extract_varchar_array('jsonl.change_details', "'$'"), 'details.detail_number') }})
        as change_detail
        , cast(jsonl.change_line_number as bigint) as sort_1
        , cast(details.detail_number as bigint) as sort_2
        , cast(0 as bigint) as sort_3
    from {{ ref('stg__google_ads__s3__change_history_api') }} as jsonl
    cross join
        {{ unnest_sequence(
            'greatest(coalesce(' ~ array_length(json_extract_varchar_array('jsonl.change_details', "'$'")) ~ ', 0), 1)'
            , 'details'
            , 'detail_number'
        ) }}
    left join report_ends on jsonl.change_course = report_ends.history_folder
    where report_ends.last_change_at is null or jsonl.change_at > report_ends.last_change_at
)

, history_details as (
    select * from report_details
    union all
    select * from jsonl_details where change_detail is not null
)

, history_changes as (
    select
        history_folder
        , change_source
        , change_at
        , {{ regexp_extract_or_null('change_detail', budget_re, 1) }} as campaign_name
        , ad_group_name
        , 'budget' as change_type
        , cast(null as varchar) as keyword_action
        , cast(null as varchar) as keyword_text
        , cast(null as varchar) as keyword_match_type
        , replace({{ regexp_extract_or_null('change_detail', budget_re, 2) }}, ',', '') as previous_value
        , replace({{ regexp_extract_or_null('change_detail', budget_re, 3) }}, ',', '') as new_value
        , sort_1
        , sort_2
        , sort_3
    from history_details
    where {{ regexp_like('change_detail', budget_re) }}

    union all

    select
        history_folder
        , change_source
        , change_at
        , campaign_name
        , ad_group_name
        , 'campaign_status' as change_type
        , null as keyword_action
        , null as keyword_text
        , null as keyword_match_type
        , lower({{ regexp_extract_or_null('change_detail', status_re, 1) }}) as previous_value
        , lower({{ regexp_extract_or_null('change_detail', status_re, 2) }}) as new_value
        , sort_1
        , sort_2
        , sort_3
    from history_details
    where
        {{ regexp_like('change_header', campaign_header_re) }}
        and {{ regexp_like('change_detail', status_re) }}

    union all

    select
        history_folder
        , change_source
        , change_at
        , campaign_name
        , ad_group_name
        , concat('campaign_', lower({{ regexp_extract_or_null('change_detail', schedule_re, 1) }})) as change_type
        , null as keyword_action
        , null as keyword_text
        , null as keyword_match_type
        , cast(cast({{ date_parse(
            regexp_extract_or_null(regexp_extract_or_null('change_detail', schedule_re, 2), schedule_date_re)
            , "'%b %d, %Y'"
        ) }} as date) as varchar) as previous_value
        , cast(cast({{ date_parse(
            regexp_extract_or_null(regexp_extract_or_null('change_detail', schedule_re, 3), schedule_date_re)
            , "'%b %d, %Y'"
        ) }} as date) as varchar) as new_value
        , sort_1
        , sort_2
        , sort_3
    from history_details
    where
        {{ regexp_like('change_header', campaign_header_re) }}
        and {{ regexp_like('change_detail', schedule_re) }}

    union all

    select
        history_folder
        , change_source
        , change_at
        , campaign_name
        , ad_group_name
        , case
            when {{ regexp_extract_or_null('change_header', keyword_header_re, 1) }} = 'negative ' then 'negative_keyword'
            else 'keyword'
        end as change_type
        , {{ regexp_extract_or_null('change_header', keyword_header_re, 3) }} as keyword_action
        , {{ google_ads_clean_keyword(
            "coalesce("
            ~ regexp_extract_or_null('change_detail', keyword_detail_re, 1) ~ ", "
            ~ regexp_extract_or_null('change_detail', keyword_detail_re, 2) ~ ", "
            ~ regexp_extract_or_null('change_detail', keyword_detail_re, 3) ~ ")"
        ) }} as keyword_text
        , upper({{ regexp_extract_or_null('change_header', keyword_header_re, 2) }}) as keyword_match_type
        , null as previous_value
        , null as new_value
        , sort_1
        , sort_2
        , sort_3
    from history_details
    where {{ regexp_like('change_header', keyword_header_re) }}

    union all

    select
        history_folder
        , change_source
        , change_at
        , campaign_name
        , ad_group_name
        , 'ad_group_status' as change_type
        , null as keyword_action
        , null as keyword_text
        , null as keyword_match_type
        , lower({{ regexp_extract_or_null('change_detail', status_re, 1) }}) as previous_value
        , lower({{ regexp_extract_or_null('change_detail', status_re, 2) }}) as new_value
        , sort_1
        , sort_2
        , sort_3
    from history_details
    where
        {{ regexp_like('change_header', ad_group_header_re) }}
        and {{ regexp_like('change_detail', status_re) }}
)

-- The API names campaigns, ad groups and keywords by resource; take the names from reports and snapshots.
, campaign_names as (
    select
        account_id
        , concat('customers/', account_id, '/campaigns/', cast(campaign_id as varchar)) as campaign_resource_name
        , latest_name as campaign_name
    from {{ ref('int__google_ads__campaign_names') }}
    where name = latest_name
)

, budget_campaigns as (
    select campaign_budget_resource_name, campaign_resource_name
    from (
        select
            campaign_budget_resource_name
            , campaign_resource_name
            , row_number() over (
                partition by campaign_budget_resource_name, campaign_resource_name order by snapshot_date desc
            ) as budget_rank
        from {{ ref('stg__google_ads__api__campaign_snapshots') }}
    ) as ranked
    where budget_rank = 1
)

, keyword_lookup as (
    select
        keyword_resource_name as criterion_resource_name
        , ad_group_name
        , keyword_text
        , keyword_match_type
        , case when keyword_is_negative then 'true' when not keyword_is_negative then 'false' end as keyword_is_negative
    from (
        select
            keyword_resource_name
            , ad_group_name
            , keyword_text
            , keyword_match_type
            , keyword_is_negative
            , row_number() over (partition by keyword_resource_name order by snapshot_date desc) as keyword_rank
        from {{ ref('stg__google_ads__api__keyword_snapshots') }}
    ) as ranked
    where keyword_rank = 1

    union all

    select
        campaign_criterion_resource_name
        , null as ad_group_name
        , keyword_text
        , keyword_match_type
        , case
            when campaign_criterion_is_negative then 'true' when not campaign_criterion_is_negative then 'false'
        end as keyword_is_negative
    from (
        select
            campaign_criterion_resource_name
            , keyword_text
            , keyword_match_type
            , campaign_criterion_is_negative
            , row_number() over (
                partition by campaign_criterion_resource_name order by snapshot_date desc
            ) as keyword_rank
        from {{ ref('stg__google_ads__api__campaign_criterion_snapshots') }}
        where keyword_text is not null
    ) as ranked
    where keyword_rank = 1
)

, ad_group_names as (
    select
        concat('customers/', account_id, '/adGroups/', cast(ad_group_id as varchar)) as ad_group_resource_name
        , max(ad_group_name) as ad_group_name
    from {{ ref('stg__google_ads__api__keyword_snapshots') }}
    group by concat('customers/', account_id, '/adGroups/', cast(ad_group_id as varchar))
)

, history_ends as (
    select history_folder, max(change_at) as last_change_at
    from history_details
    group by history_folder
)

, api_events as (
    select
        history_accounts.history_folder
        , events.change_event_at as change_at
        , events.change_resource_type
        , events.change_operation
        , events.change_resource_name
        , concat(',', events.change_changed_fields, ',') as changed_fields
        , nullif(events.campaign_resource_name, '') as campaign_resource_name
        , nullif(events.ad_group_resource_name, '') as ad_group_resource_name
        , events.change_old_resource as old_resource
        , events.change_new_resource as new_resource
        , row_number() over (
            partition by events.account_id
            order by
                events.change_event_at nulls last
                , events.change_resource_name nulls last
                , events.change_operation nulls last
                , events.change_changed_fields nulls last
        ) as event_order
    from {{ ref('stg__google_ads__api__change_events') }} as events
    left join history_accounts on events.account_id = history_accounts.account_id
    left join history_ends on history_accounts.history_folder = history_ends.history_folder
    where history_ends.last_change_at is null or events.change_event_at > history_ends.last_change_at
)

, api_changes as (
    select
        api_events.history_folder
        , 'api' as change_source
        , api_events.change_at
        , coalesce(campaign_names.campaign_name, {{ json_query_string('api_events.new_resource', "'$.campaign.name'") }})
        as campaign_name
        , null as ad_group_name
        , 'campaign_status' as change_type
        , null as keyword_action
        , null as keyword_text
        , null as keyword_match_type
        , lower({{ json_query_string('api_events.old_resource', "'$.campaign.status'") }}) as previous_value
        , lower({{ json_query_string('api_events.new_resource', "'$.campaign.status'") }}) as new_value
        , api_events.event_order as sort_1
        , cast(0 as bigint) as sort_2
        , cast(0 as bigint) as sort_3
    from api_events
    left join campaign_names on api_events.campaign_resource_name = campaign_names.campaign_resource_name
    where
        api_events.change_resource_type = 'CAMPAIGN'
        and (api_events.change_operation = 'CREATE' or api_events.changed_fields like '%,status,%')

    union all

    select
        api_events.history_folder
        , 'api' as change_source
        , api_events.change_at
        , coalesce(campaign_names.campaign_name, {{ json_query_string('api_events.new_resource', "'$.campaign.name'") }})
        as campaign_name
        , null as ad_group_name
        , 'campaign_end' as change_type
        , null as keyword_action
        , null as keyword_text
        , null as keyword_match_type
        -- end_date_time is "YYYY-MM-DD HH:MM:SS"; its first 10 characters are the date.
        , substr(nullif({{ json_query_string('api_events.old_resource', "'$.campaign.end_date_time'") }}, ''), 1, 10)
        as previous_value
        , substr(nullif({{ json_query_string('api_events.new_resource', "'$.campaign.end_date_time'") }}, ''), 1, 10)
        as new_value
        , api_events.event_order as sort_1
        , cast(0 as bigint) as sort_2
        , cast(0 as bigint) as sort_3
    from api_events
    left join campaign_names on api_events.campaign_resource_name = campaign_names.campaign_resource_name
    where
        api_events.change_resource_type = 'CAMPAIGN'
        and api_events.changed_fields like '%,end_date_time,%'

    union all

    -- A shared budget's event has no campaign; it applies to every campaign using the budget.
    select
        api_events.history_folder
        , 'api' as change_source
        , api_events.change_at
        , campaign_names.campaign_name
        , null as ad_group_name
        , 'budget' as change_type
        , null as keyword_action
        , null as keyword_text
        , null as keyword_match_type
        , case when api_events.change_operation = 'UPDATE' then cast(
            {{ google_ads_micros(json_query_string('api_events.old_resource', "'$.campaign_budget.amount_micros'")) }}
            as varchar
        ) end as previous_value
        , cast(
            {{ google_ads_micros(json_query_string('api_events.new_resource', "'$.campaign_budget.amount_micros'")) }}
            as varchar
        ) as new_value
        , api_events.event_order as sort_1
        , cast(0 as bigint) as sort_2
        , cast(0 as bigint) as sort_3
    from api_events
    left join budget_campaigns
        on
            api_events.campaign_resource_name is null
            and api_events.change_resource_name = budget_campaigns.campaign_budget_resource_name
    left join campaign_names
        on coalesce(api_events.campaign_resource_name, budget_campaigns.campaign_resource_name)
        = campaign_names.campaign_resource_name
    where
        api_events.change_resource_type = 'CAMPAIGN_BUDGET'
        and (api_events.change_operation = 'CREATE' or api_events.changed_fields like '%,amount_micros,%')

    union all

    select
        api_events.history_folder
        , 'api' as change_source
        , api_events.change_at
        , campaign_names.campaign_name
        , coalesce(
            {{ json_query_string('api_events.new_resource', "'$.ad_group.name'") }}, ad_group_names.ad_group_name
        ) as ad_group_name
        , 'ad_group_status' as change_type
        , null as keyword_action
        , null as keyword_text
        , null as keyword_match_type
        , lower({{ json_query_string('api_events.old_resource', "'$.ad_group.status'") }}) as previous_value
        , lower({{ json_query_string('api_events.new_resource', "'$.ad_group.status'") }}) as new_value
        , api_events.event_order as sort_1
        , cast(0 as bigint) as sort_2
        , cast(0 as bigint) as sort_3
    from api_events
    left join campaign_names on api_events.campaign_resource_name = campaign_names.campaign_resource_name
    left join ad_group_names on api_events.change_resource_name = ad_group_names.ad_group_resource_name
    where
        api_events.change_resource_type = 'AD_GROUP'
        and (api_events.change_operation = 'CREATE' or api_events.changed_fields like '%,status,%')
)

, api_criteria as (
    select
        api_events.history_folder
        , api_events.change_at
        , api_events.change_resource_type
        , api_events.change_operation
        , api_events.changed_fields
        , api_events.event_order
        , campaign_names.campaign_name
        , case
            when api_events.change_resource_type = 'AD_GROUP_CRITERION'
                then coalesce(keyword_lookup.ad_group_name, ad_group_names.ad_group_name)
        end as ad_group_name
        -- A REMOVE event carries the keyword only in old_resource.
        , coalesce(
            {{ json_query_string('api_events.new_resource', "'$.ad_group_criterion.keyword.text'") }}
            , {{ json_query_string('api_events.new_resource', "'$.campaign_criterion.keyword.text'") }}
            , {{ json_query_string('api_events.old_resource', "'$.ad_group_criterion.keyword.text'") }}
            , {{ json_query_string('api_events.old_resource', "'$.campaign_criterion.keyword.text'") }}
            , keyword_lookup.keyword_text
        ) as raw_keyword_text
        , coalesce(
            {{ json_query_string('api_events.new_resource', "'$.ad_group_criterion.keyword.match_type'") }}
            , {{ json_query_string('api_events.new_resource', "'$.campaign_criterion.keyword.match_type'") }}
            , {{ json_query_string('api_events.old_resource', "'$.ad_group_criterion.keyword.match_type'") }}
            , {{ json_query_string('api_events.old_resource', "'$.campaign_criterion.keyword.match_type'") }}
            , keyword_lookup.keyword_match_type
        ) as raw_match_type
        , coalesce(
            {{ json_query_string('api_events.new_resource', "'$.ad_group_criterion.negative'") }}
            , {{ json_query_string('api_events.new_resource', "'$.campaign_criterion.negative'") }}
            , {{ json_query_string('api_events.old_resource', "'$.ad_group_criterion.negative'") }}
            , {{ json_query_string('api_events.old_resource', "'$.campaign_criterion.negative'") }}
            , keyword_lookup.keyword_is_negative
            , case when api_events.change_resource_type = 'CAMPAIGN_CRITERION' then 'true' else 'false' end
        ) = 'true' as is_negative
        , coalesce(
            {{ json_query_string('api_events.new_resource', "'$.ad_group_criterion.status'") }}
            , {{ json_query_string('api_events.new_resource', "'$.campaign_criterion.status'") }}
        ) as new_status
    from api_events
    left join campaign_names on api_events.campaign_resource_name = campaign_names.campaign_resource_name
    left join keyword_lookup on api_events.change_resource_name = keyword_lookup.criterion_resource_name
    left join ad_group_names on api_events.ad_group_resource_name = ad_group_names.ad_group_resource_name
    where api_events.change_resource_type in ('AD_GROUP_CRITERION', 'CAMPAIGN_CRITERION')
)

, api_keyword_changes as (
    select
        history_folder
        , 'api' as change_source
        , change_at
        , campaign_name
        , ad_group_name
        , case when is_negative then 'negative_keyword' else 'keyword' end as change_type
        , case
            when change_operation = 'CREATE' then 'added'
            when change_operation = 'REMOVE' then 'removed'
            when changed_fields like '%,status,%' then lower(new_status)
        end as keyword_action
        , {{ google_ads_clean_keyword('raw_keyword_text') }} as keyword_text
        -- A keyword typed with brackets or quotes arrives with them and no match type.
        , case
            when raw_match_type in ('EXACT', 'PHRASE', 'BROAD') then raw_match_type
            when raw_keyword_text like '[%]' then 'EXACT'
            when raw_keyword_text like '"%"' then 'PHRASE'
            -- Without either, it is Broad: every such keyword with a snapshot is.
            when raw_match_type is null then 'BROAD'
        end as keyword_match_type
        , null as previous_value
        , null as new_value
        , event_order as sort_1
        , cast(0 as bigint) as sort_2
        , cast(0 as bigint) as sort_3
    from api_criteria
    where nullif(raw_keyword_text, '') is not null
)

, all_changes as (
    select * from history_changes
    union all
    select * from api_changes
    union all
    select * from api_keyword_changes
)

-- Report and JSONL names are as of their download; a later rename maps them to the current
-- name (current_names). A name two campaigns in one account had is left as it is.
, name_map as (
    select names.account_id, names.name, max(names.latest_name) as latest_name
    from {{ ref('int__google_ads__campaign_names') }} as names
    group by names.account_id, names.name
    having count(distinct names.latest_name) = 1
)

-- History rows name their campaign but not its id, so courses are matched by latest name.
, name_courses as (
    select campaign_name, max(course) as course
    from {{ ref('int__google_ads__campaign_courses') }}
    group by campaign_name
    having count(distinct course) = 1
)

, current_changes as (
    select
        all_changes.change_source
        , all_changes.change_at
        , coalesce(name_map.latest_name, all_changes.campaign_name) as campaign_name
        , all_changes.ad_group_name
        , all_changes.change_type
        , all_changes.keyword_action
        , all_changes.keyword_text
        , all_changes.keyword_match_type
        , all_changes.previous_value
        , all_changes.new_value
        , all_changes.sort_1
        , all_changes.sort_2
        , all_changes.sort_3
    from all_changes
    left join history_accounts on all_changes.history_folder = history_accounts.history_folder
    left join name_map
        on history_accounts.account_id = name_map.account_id and all_changes.campaign_name = name_map.name
)

select
    name_courses.course
    , current_changes.change_source
    , current_changes.change_at
    , current_changes.campaign_name
    , nullif(current_changes.ad_group_name, '') as ad_group_name
    , current_changes.change_type
    , current_changes.keyword_action
    , current_changes.keyword_text
    , case when current_changes.keyword_match_type is not null
        then concat(
            upper(substr(current_changes.keyword_match_type, 1, 1))
            , lower(substr(current_changes.keyword_match_type, 2))
        )
    end as keyword_match_type
    , current_changes.previous_value
    , current_changes.new_value
    -- Same-second changes keep their source order, as ad_opt_v2's stable sort does.
    , row_number() over (
        partition by name_courses.course
        order by
            current_changes.change_at nulls last
            , current_changes.sort_1 nulls last
            , current_changes.sort_2 nulls last
            , current_changes.sort_3 nulls last
    ) as change_order
from current_changes
left join name_courses on current_changes.campaign_name = name_courses.campaign_name
-- parse_change_history's Search campaigns: the name says Search and not Experiment.
where
    current_changes.campaign_name like '%Search%'
    and current_changes.campaign_name not like '%Experiment%'
    -- A keyword update that is not a status change (a bid, a URL) changes no list.
    and (
        current_changes.change_type not in ('keyword', 'negative_keyword')
        or current_changes.keyword_action in ('added', 'removed', 'paused', 'enabled')
    )
