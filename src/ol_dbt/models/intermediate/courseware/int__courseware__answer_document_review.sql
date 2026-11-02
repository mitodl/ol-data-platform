{{ config(enabled=courseware_local_tables_enabled('courseware_answer_check_in_warehouse')) }}

{#
    The course documents a person should look at to keep answer keys away from the
    claim extractor, one row per document:

      excluded_by_rule  courseware_answer_key reads it as an answer key, so it is
                        never read. Spot-check these for teaching material dropped
                        by mistake.
      flagged_by_model  the answer check (ml.lib.courseware.answer_check) read an
                        extraction source as mainly answers. It is still read until
                        a person confirms it; these are the keys the rule missed.

    Open edX documents are named by their path without its extension and have no
    title. OCW resources are judged by courseware_ocw_answer_resource, by their
    learning resource types and their directory name; pages are not listed, since a
    page's answers are cut from it rather than the page dropped. A source's latest
    verdict is the one shown, and only while the source is still read or the rule
    excludes it: the verdict log is append-only, so a document since removed from
    the export would otherwise stay flagged.
#}
{% set courseruns = var('courseware_courseruns', []) %}
{% set courserun_filter = "('" ~ courseruns | join("', '") ~ "')" if courseruns else none %}

with openedx_documents as (
    select
        case content_file_source_system
            when 'xpro' then 'mitxpro'
            when 'mitx' then 'residential'
            else content_file_source_system
        end as platform
        , courserun_readable_id
        , content_file_path as source_ref
        , {{ regexp_replace_all('content_file_path', "'[.][A-Za-z0-9]+$'", "''") }} as source_name
        , '' as source_title
        , {{ courseware_text_head('content_file_text') }} as text_head
        , {{ courseware_answer_markers('content_file_text') }} as answer_markers
    from {{ ref('stg__openedx__s3__course_document_text') }}
    where
        content_file_extraction_status = 'extracted'
        -- an html block's body is read as the block, not judged as a file
        and content_file_path not like 'html/%'
        {% if courserun_filter %}and courserun_readable_id in {{ courserun_filter }}{% endif %}
)

, ocw_resources as (
    select
        'ocw' as platform
        , courserun_readable_id
        , content_file_key as source_ref
        , {{ courseware_ocw_resource_name('content_file_key') }} as source_name
        , lower(coalesce(title, '')) as source_title
        , {{ courseware_text_head('content') }} as text_head
        , {{ courseware_answer_markers('content') }} as answer_markers
        , content_type
        , file_extension
        , content_tags
    from {{ ref('int__ocw__content_files') }}
    where
        courserun_readable_id is not null
        and content_type != 'page'
        {% if courserun_filter %}and courserun_readable_id in {{ courserun_filter }}{% endif %}
)

, rule_exclusions as (
    select
        platform
        , courserun_readable_id
        , source_ref
        , source_name
        , source_title
        , text_head
    from openedx_documents
    where {{ courseware_answer_key('source_name', 'source_title', 'text_head', 'answer_markers') }}

    union all

    select
        platform
        , courserun_readable_id
        , source_ref
        , source_name
        , source_title
        , text_head
    from ocw_resources
    where {{ courseware_ocw_answer_resource(
        'source_name', 'source_title', 'content_type', 'file_extension', 'content_tags', 'text_head', 'answer_markers'
    ) }}
)

, latest_checks as (
    select
        platform
        , courserun_readable_id
        , source_ref
        , source_title
        , is_answer_document
        , reason
        , check_version
        , model_id
        , checked_at
        , row_number() over (
            partition by platform, courserun_readable_id, source_ref
            order by checked_at desc, check_version desc
        ) as check_rank
    from {{ source('courseware_intermediate', 'courseware_answer_check') }}
    {% if courserun_filter %}where courserun_readable_id in {{ courserun_filter }}{% endif %}
)

, model_flags as (
    select *
    from latest_checks
    where check_rank = 1 and is_answer_document
)

, current_sources as (
    select distinct
        platform
        , courserun_readable_id
        , source_ref
    from {{ ref('int__courseware__extraction_sources') }}
)

select
    coalesce(rule_exclusions.platform, model_flags.platform) as platform
    , coalesce(rule_exclusions.courserun_readable_id, model_flags.courserun_readable_id) as courserun_readable_id
    , coalesce(rule_exclusions.source_ref, model_flags.source_ref) as source_ref
    , rule_exclusions.source_name
    , coalesce(nullif(rule_exclusions.source_title, ''), model_flags.source_title) as source_title
    , rule_exclusions.text_head
    , case
        when rule_exclusions.source_ref is not null then 'excluded_by_rule'
        else 'flagged_by_model'
    end as review_reason
    , rule_exclusions.source_ref is not null as is_excluded_by_rule
    , model_flags.source_ref is not null as is_flagged_by_model
    , model_flags.reason as model_reason
    , model_flags.check_version
    , model_flags.model_id
    , model_flags.checked_at
from rule_exclusions
full outer join model_flags
    on
        rule_exclusions.platform = model_flags.platform
        and rule_exclusions.courserun_readable_id = model_flags.courserun_readable_id
        and rule_exclusions.source_ref = model_flags.source_ref
left join current_sources
    on
        model_flags.platform = current_sources.platform
        and model_flags.courserun_readable_id = current_sources.courserun_readable_id
        and model_flags.source_ref = current_sources.source_ref
where rule_exclusions.source_ref is not null or current_sources.source_ref is not null
