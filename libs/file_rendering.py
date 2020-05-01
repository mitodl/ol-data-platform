# -*- coding: utf-8 -*-

import csv
from pathlib import Path
from typing import Dict, List, Text


def write_csv(table_fields: List[Text], table_data: List[Dict], dest_file: Path) -> Path:
    """Write table data in CSV format to a given path or to a tempfile.

    :param table_data: Tabular data formatted as a list of dictionaries
    :type table_data: List[Dict]

    :param dest_file: Destination file for CSV data

    :returns: Location of the file that data is written to

    :rtype: Path
    """
    with dest_file.open(mode='w') as outfile:
        writer = csv.DictWriter(outfile, table_fields)
        writer.writeheader()
        for table_row in table_data:
            writer.writerow(table_row)
        return dest_file
