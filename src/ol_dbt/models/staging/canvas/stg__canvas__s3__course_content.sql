{% set raw_course_content = source("ol_warehouse_raw_data", "raw__canvas__s3__course_content") %}
{% set member_fact_columns = {
    "parse_rule": dbt.type_bigint(),
    "canvas_domain": dbt.type_string(),
    "canvas_course_id": dbt.type_string(),
    "files_tab_hidden": dbt.type_boolean(),
    "public_syllabus": dbt.type_string(),
    "public_syllabus_to_auth": dbt.type_string(),
    "member_path": dbt.type_string(),
    "pdf_is_valid": dbt.type_boolean(),
    "in_tutor_folder": dbt.type_boolean(),
    "draft_in_parent_path": dbt.type_boolean(),
    "folder_restricted": dbt.type_boolean(),
    "files_display_name": dbt.type_string(),
    "files_hidden": dbt.type_boolean(),
    "files_locked": dbt.type_boolean(),
    "files_visibility": dbt.type_string(),
    "files_lock_at": dbt.type_timestamp(),
    "files_unlock_at": dbt.type_timestamp(),
    "module_items": dbt.type_string(),
    "page_title": dbt.type_string(),
    "page_workflow_state": dbt.type_string(),
    "page_is_assignment": dbt.type_boolean(),
    "page_intended_role": dbt.type_string(),
    "page_is_syllabus": dbt.type_boolean(),
    "page_lock_at": dbt.type_timestamp(),
    "page_unlock_at": dbt.type_timestamp(),
    "page_only_visible_to_overrides": dbt.type_boolean(),
    "page_hide_in_gradebook": dbt.type_boolean(),
    "page_module_locked": dbt.type_boolean(),
    "linked_from_pages": dbt.type_string(),
    "api_published": dbt.type_boolean(),
    "api_hidden": dbt.type_boolean(),
    "api_locked": dbt.type_boolean(),
    "api_lock_at": dbt.type_timestamp(),
    "api_unlock_at": dbt.type_timestamp(),
    "api_folder_hidden": dbt.type_boolean(),
    "api_folder_locked": dbt.type_boolean(),
    "api_url": dbt.type_string(),
    "learn_published": dbt.type_boolean(),
    "learn_key": dbt.type_string(),
    "learn_title": dbt.type_string(),
    "learn_content_title": dbt.type_string(),
    "learn_url": dbt.type_string(),
} %}

with
    -- The load adds these columns with the first course it reads under parse rule
    -- 2; until then they are null, and so the model has no rows.
    raw_rows as (select *{{ missing_columns_as_nulls(raw_course_content, member_fact_columns) }} from {{ raw_course_content }}),
    -- Rows from before parse rule 2 have another shape (one row per file Learn
    -- would load). Every course is read again under the new rule, so they are left
    -- out rather than carried.
    source as (select * from raw_rows where parse_rule >= 2 or content_kind = 'unpublished'),
    -- The load appends a full set of rows each time a course changes, all stamped
    -- with one course_retrieved_at. The newest set is the course: a file removed
    -- from it is still in the older sets, so deduplicating per row would keep it.
    newest as (select course_id, max(course_retrieved_at) as course_retrieved_at from source group by course_id),
    -- A course is read whole again to retry a file Tika failed on, and a file that
    -- extracted before can fail on the retry. Its bytes have not changed while its
    -- MD5 has not, so its last extracted text still stands. Only the files failed
    -- in a newest set are looked up, so the cost does not grow with the table's
    -- history.
    failed as (
        select source.course_id, source.member_path, source.file_md5
        from source
        inner join
            newest on source.course_id = newest.course_id and source.course_retrieved_at = newest.course_retrieved_at
        where source.extraction_status = 'failed'
    ),
    extracted as (
        select
            source.course_id,
            source.member_path,
            source.file_md5,
            source.content,
            row_number() over (
                partition by source.course_id, source.member_path, source.file_md5
                order by source.course_retrieved_at desc
            ) as read_rank
        from source
        inner join
            failed
            on source.course_id = failed.course_id
            and source.member_path = failed.member_path
            and source.file_md5 = failed.file_md5
        where source.extraction_status = 'extracted'
    )

select
    source.course_id,
    source.content_kind as coursecontent_kind,
    source.archive_key as coursecontent_archive_key,
    source.course_code,
    source.course_title,
    source.canvas_domain as course_canvas_domain,
    source.canvas_course_id as course_canvas_id,
    source.course_start_at as course_start_on,
    source.course_end_at as course_end_on,
    source.files_tab_hidden as course_files_tab_hidden,
    source.public_syllabus as course_public_syllabus,
    source.public_syllabus_to_auth as course_public_syllabus_to_auth,
    source.member_path as coursecontent_member_path,
    source.file_extension as coursecontent_file_extension,
    source.file_md5 as coursecontent_file_md5,
    source.file_size_bytes as coursecontent_file_size_bytes,
    coalesce(extracted.content, source.content) as coursecontent_text,
    case
        when extracted.content is not null then 'extracted' else source.extraction_status
    end as coursecontent_text_extraction_status,
    source.extraction_status as coursecontent_text_read_status,
    source.pdf_is_valid as coursecontent_pdf_is_valid,
    source.in_tutor_folder as coursecontent_in_tutor_folder,
    source.draft_in_parent_path as coursecontent_draft_in_parent_path,
    source.folder_restricted as coursecontent_folder_restricted,
    source.files_display_name as coursecontent_files_display_name,
    source.files_hidden as coursecontent_files_hidden,
    source.files_locked as coursecontent_files_locked,
    source.files_visibility as coursecontent_files_visibility,
    {{ cast_timestamp_to_iso8601("source.files_lock_at") }} as coursecontent_files_lock_on,
    {{ cast_timestamp_to_iso8601("source.files_unlock_at") }} as coursecontent_files_unlock_on,
    source.module_items as coursecontent_module_items,
    source.page_title as coursecontent_page_title,
    source.page_workflow_state as coursecontent_page_workflow_state,
    source.page_is_assignment as coursecontent_page_is_assignment,
    source.page_intended_role as coursecontent_page_intended_role,
    source.page_is_syllabus as coursecontent_page_is_syllabus,
    {{ cast_timestamp_to_iso8601("source.page_lock_at") }} as coursecontent_page_lock_on,
    {{ cast_timestamp_to_iso8601("source.page_unlock_at") }} as coursecontent_page_unlock_on,
    source.page_only_visible_to_overrides as coursecontent_page_only_visible_to_overrides,
    source.page_hide_in_gradebook as coursecontent_page_hide_in_gradebook,
    source.page_module_locked as coursecontent_page_module_locked,
    source.linked_from_pages as coursecontent_linked_from_pages,
    source.api_published as coursecontent_api_published,
    source.api_hidden as coursecontent_api_hidden,
    source.api_locked as coursecontent_api_locked,
    {{ cast_timestamp_to_iso8601("source.api_lock_at") }} as coursecontent_api_lock_on,
    {{ cast_timestamp_to_iso8601("source.api_unlock_at") }} as coursecontent_api_unlock_on,
    source.api_folder_hidden as coursecontent_api_folder_hidden,
    source.api_folder_locked as coursecontent_api_folder_locked,
    source.api_url as coursecontent_api_url,
    source.learn_published as coursecontent_learn_published,
    source.learn_key as coursecontent_learn_key,
    source.learn_title as coursecontent_learn_title,
    source.learn_content_title as coursecontent_learn_content_title,
    source.learn_url as coursecontent_learn_url,
    {{ cast_timestamp_to_iso8601("source.course_retrieved_at") }} as coursecontent_retrieved_on
from source
inner join newest on source.course_id = newest.course_id and source.course_retrieved_at = newest.course_retrieved_at
left join
    extracted
    on source.extraction_status = 'failed'
    and source.course_id = extracted.course_id
    and source.member_path = extracted.member_path
    and source.file_md5 = extracted.file_md5
    and extracted.read_rank = 1
-- A course that left the landing zone loads one "unpublished" row so that it
-- becomes the newest set; the row itself is not content.
where source.content_kind != 'unpublished'
