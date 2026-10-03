{#
  newest_course_file_rows: the rows of each course's newest landed file.

  The openedx code location lands one JSON Lines file per course version and the
  dlt load appends every file, so a raw table holds every version of every
  course. A row that disappeared from a course is still there in the older file,
  so deduplicating per row would keep it forever; the course's newest file is the
  course's current state. S3 mtimes are whole seconds, so two versions can tie on
  _file_modified_at. Pass extracted_at_column when the producer stamps its rows
  with when it ran, and that breaks the tie; the file path, a content hash that
  orders nothing, only makes the pick deterministic after that. ISO 8601 UTC
  strings sort chronologically, so the column is compared as text.

  File names are content hashes, and the asset rewrites the same key when a
  re-run produces the same text, which gives it a new mtime and gets it appended
  again. Only the copy loaded from the newest mtime is kept, or every row of that
  file would come back twice.

  Usage, as a CTE body:
    with source as ({{ newest_course_file_rows(source('ol_warehouse_raw_data', 'raw__x')) }})
#}
{% macro newest_course_file_rows(relation, extracted_at_column=none) %}
    select raw_rows.*
    from {{ relation }} as raw_rows
    inner join (
        select source_system, course_id, _source_file, file_modified_at
        from (
            select
                source_system
                , course_id
                , _source_file
                , max(_file_modified_at) as file_modified_at
                , row_number() over (
                    partition by source_system, course_id
                    order by
                        max(_file_modified_at) desc
                        {%- if extracted_at_column is not none %}
                        , max({{ extracted_at_column }}) desc nulls last
                        {%- endif %}
                        , _source_file desc
                ) as file_rank
            from {{ relation }}
            group by source_system, course_id, _source_file
        ) as course_files
        where file_rank = 1
    ) as newest_files
        on raw_rows.source_system = newest_files.source_system
        and raw_rows.course_id = newest_files.course_id
        and raw_rows._source_file = newest_files._source_file
        and raw_rows._file_modified_at = newest_files.file_modified_at
{% endmacro %}
