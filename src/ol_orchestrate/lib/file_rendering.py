import csv
from pathlib import Path


def write_csv(
    table_fields: list[str], table_data: list[dict[str, str]], dest_file: Path
) -> Path:
    """Write table data in CSV format to a given path or to a tempfile.

    :param table_fields: List of column names for the table being rendered as CSV
    :type table_fields: List[str]

    :param table_data: Tabular data formatted as a list of dictionaries
    :type table_data: List[Dict]

    :param dest_file: Destination file for CSV data
    :type dest_file: Path

    :returns: Location of the file that data is written to

    :rtype: Path
    """
    with dest_file.open(mode="a") as outfile:
        writer = csv.DictWriter(outfile, table_fields)
        if outfile.tell() <= 0:
            writer.writeheader()
        writer.writerows(table_data)
        return dest_file
