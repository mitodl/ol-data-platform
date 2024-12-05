import dlt
from dlt.sources.csv import csv_source
from dlt.destinations.filesystem import filesystem_destination

# Define the source and destination configurations
source_config = {
    "path": "s3://your-input-bucket/your-csv-path/*.csv"
}

destination_config = {
    "path": "s3://your-output-bucket/your-jsonl-path/output.jsonl",
    "format": "jsonl"
}

# Define the pipeline using DLT
@dlt.pipeline(
    source=csv_source(source_config),
    destination=filesystem_destination(destination_config)
)
def edxorg_table_data():
    return dlt.resource("edxorg_table_data")

if __name__ == "__main__":
    # Run the pipeline
    pipeline = edxorg_table_data()
    pipeline.run()
