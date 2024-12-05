import dlt
import os
from dlt.sources.filesystem import filesystem_source

# Configure AWS credentials
# Recommended: Use environment variables or AWS credentials file
# export AWS_ACCESS_KEY_ID='your_access_key'
# export AWS_SECRET_ACCESS_KEY='your_secret_key'

# Define a pipeline to load CSV files from S3
@dlt.source(name="s3_csv_source")
def s3_csv_files(
    s3_path: str = "s3://your-bucket/path/to/csv/files/",
    file_pattern: str = "*.csv"
):
    """
    Source function to read CSV files from S3

    Args:
        s3_path: Full S3 path to the directory containing CSV files
        file_pattern: File pattern to match (default *.csv)

    Returns:
        A filesystem source that can be loaded by dlt
    """
    return filesystem_source(
        path=s3_path,
        file_pattern=file_pattern,
        mode="csv",  # Specifies CSV parsing
        ignore_columns_with_invalid_utf8=True,  # Handle potential encoding issues
        infer_schema=True  # Automatically infer schema
    )

# Create the pipeline
pipeline = dlt.pipeline(
    pipeline_name="s3_csv_to_jsonl_pipeline",
    destination={
        "type": "filesystem",
        "path": "s3://your-bucket/path/to/output/jsonl/files/",
        "file_format": "jsonl"  # Specify JSONL format
    },
    dataset_name="csv_data"
)

# Run the pipeline
def run_pipeline():
    """
    Execute the pipeline to load CSV files and write as JSONL
    """
    try:
        # Load source and write to destination
        load_info = pipeline.run(
            s3_csv_files(),
            write_disposition="replace"  # Replaces existing data
        )

        # Print load information
        print(f"Pipeline completed. Loaded {load_info.row_count} rows.")
        print(f"Destination path: {pipeline.dataset_destination}")

    except Exception as e:
        print(f"Pipeline execution failed: {e}")

# Optional: Main block for direct script execution
if __name__ == "__main__":
    run_pipeline()

# Recommended additional configuration
# Configure logging
import logging
dlt.configure(
    log_level=logging.INFO,
    telemetry_level="none"  # Disable telemetry if desired
)
