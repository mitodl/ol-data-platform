"""Helper class for parquet data output."""
from pyarrow import concat_tables, parquet


def write_parquet_file(file_system, output_folder, arrow_table, file_name):
    """Output an arrow table as a parquet file.

    :file_system: pyarrow interchangable filesystem. May be local, S3, etc.
    :type file_system: pyarrow FileSystem type

    :output_folder: Folder for outputs
    :type output_folder: String

    :arrow_table: Arrow table to be written as parquet file
    :type arrow_table: pyarrow.Table

    :file_name: Name for parquet file
    :type file_name: String
    """
    file_path = "{output_folder}/{file_name}.parquet".format(
        output_folder=output_folder, file_name=file_name
    )

    if file_system.type_name == "local":
        file_system.create_dir(output_folder)

    with file_system.open_output_stream(file_path) as parquet_file:  # noqa: SIM117
        with parquet.ParquetWriter(
            parquet_file, arrow_table.schema, use_deprecated_int96_timestamps=True
        ) as writer:
            writer.write_table(arrow_table)


def stream_to_parquet_file(
    arrow_table_generator, file_base, file_system, output_folder
):
    """Write to numbered parquet files from a generator.

    :arrow_table_generator: generator that yeilds arrow tables
    :type arrow_table_generator: Generator

    :file_base: base name for parquet files
    :type file_base: String

    :file_system: pyarrow interchangable filesystem. May be local, S3, etc.
    :type file_system: pyarrow FileSystem type

    :output_folder: Folder for outputs
    :type output_folder: String

    :returns: arrow shema for parquet file data
    :rtype: Schema
    """
    arrow_table = concat_tables(arrow_table_generator)
    write_parquet_file(file_system, output_folder, arrow_table, file_base)
    return arrow_table.schema
