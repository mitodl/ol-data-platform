with
    source as (select * from {{ source("ol_warehouse_raw_data", "raw__canvas__s3__course_content") }}),
    -- The load appends a full set of rows each time a course changes, all stamped
    -- with one course_retrieved_at. The newest set is the course: a file removed
    -- from it, or locked since, is still in the older sets, so deduplicating per
    -- row would keep it.
    newest as (select course_id, max(course_retrieved_at) as course_retrieved_at from source group by course_id),
    -- A course is read whole again to retry a file Tika failed on, and a file that
    -- extracted before can fail on the retry. Its bytes have not changed while its
    -- MD5 has not, so its last extracted text still stands. Only the files failed
    -- in a newest set are looked up, so the cost does not grow with the table's
    -- history.
    failed as (
        select source.course_id, source.source_path, source.file_md5
        from source
        inner join
            newest on source.course_id = newest.course_id and source.course_retrieved_at = newest.course_retrieved_at
        where source.extraction_status = 'failed'
    ),
    extracted as (
        select
            source.course_id,
            source.source_path,
            source.file_md5,
            source.content,
            row_number() over (
                partition by source.course_id, source.source_path, source.file_md5
                order by source.course_retrieved_at desc
            ) as read_rank
        from source
        inner join
            failed
            on source.course_id = failed.course_id
            and source.source_path = failed.source_path
            and source.file_md5 = failed.file_md5
        where source.extraction_status = 'extracted'
    )

select
    source.course_id,
    source.content_kind as coursecontent_kind,
    source.archive_key as coursecontent_archive_key,
    source.run_readable_id as courserun_readable_id,
    source.course_code,
    source.course_title,
    source.course_url,
    source.course_start_at as course_start_on,
    source.course_end_at as course_end_on,
    source.file_key as coursecontent_key,
    source.source_path as coursecontent_source_path,
    source.file_extension as coursecontent_file_extension,
    source.title as coursecontent_title,
    source.content_title as coursecontent_content_title,
    source.url as coursecontent_url,
    source.file_md5 as coursecontent_file_md5,
    source.file_size_bytes as coursecontent_file_size_bytes,
    coalesce(extracted.content, source.content) as coursecontent_text,
    case
        when extracted.content is not null then 'extracted' else source.extraction_status
    end as coursecontent_text_extraction_status,
    {{ cast_timestamp_to_iso8601("source.course_retrieved_at") }} as coursecontent_retrieved_on
from source
inner join newest on source.course_id = newest.course_id and source.course_retrieved_at = newest.course_retrieved_at
left join
    extracted
    on source.extraction_status = 'failed'
    and source.course_id = extracted.course_id
    and source.source_path = extracted.source_path
    and source.file_md5 = extracted.file_md5
    and extracted.read_rank = 1
-- A course that left the landing zone loads one "unpublished" row so that it
-- becomes the newest set; the row itself is not content.
where source.content_kind != 'unpublished'
