{#
    Every table row on the pages of the OCW courses var('courseware_ocw_courses')
    selects, its table's header row included. int__courseware__ocw_resources reads the
    header's answer-key columns from it, and int__courseware__extraction_sources the
    rows that are sessions. A table of its own for the reason int__courseware__ocw_files
    is.
#}

-- Every page's tables, one row per table row. On top-level section pages and the
-- calendar, a row whose first cell is a number, in a table whose first header names
-- sessions, lectures or weeks, is that session's row.
with ocw_row_pages as (
    select
        pages.courserun_readable_id
        , pages.content_file_key as page_key
        , pages.section
        , coalesce(pages.is_flat_section, false) or pages.section = 'calendar' as reads_sessions
        , {{ regexp_split('pages.body', "'<table'") }} as tables
    from {{ ref('int__courseware__ocw_files') }} as pages
    where
        pages.page_path is not null
        and pages.body like '%<table%'
)

, ocw_tables as (
    select
        row_pages.courserun_readable_id
        , row_pages.page_key
        , row_pages.section
        , row_pages.reads_sessions
        , page_tables.table_index
        , {{ regexp_split(
            split_part_or_null(element_at_array('row_pages.tables', 'page_tables.table_index'), "'</table>'", 1)
            , "'<tr'"
        ) }} as table_rows
    from ocw_row_pages as row_pages
    cross join {{ unnest_sequence(array_length('row_pages.tables'), 'page_tables', 'table_index') }}
    where page_tables.table_index > 1
)

select
    tables.courserun_readable_id
    , tables.page_key
    , tables.section
    , tables.reads_sessions
    , tables.table_index
    , table_rows.row_index
    , {{ element_at_array('tables.table_rows', 'table_rows.row_index') }} as row_html
    -- the table's first cell, its header
    , {{ regexp_extract_or_null(element_at_array('tables.table_rows', 2), courseware_ocw_table_cell(), 1) }} as header_html
from ocw_tables as tables
cross join {{ unnest_sequence(array_length('tables.table_rows'), 'table_rows', 'row_index') }}
where table_rows.row_index > 1
