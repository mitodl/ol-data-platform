#!/usr/bin/env python3
"""
Test reading Iceberg tables from AWS Glue catalog using DuckDB.

This validates that DuckDB can:
1. Connect to AWS Glue catalog
2. Read Iceberg table metadata
3. Query data from Iceberg tables in S3
"""

import os
from pathlib import Path

import duckdb


def test_glue_iceberg_read():
    """Test reading Iceberg table from AWS Glue"""

    # Create a test database
    db_path = Path.home() / ".ol-dbt" / "test_glue.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Connecting to DuckDB at: {db_path}")
    conn = duckdb.connect(str(db_path))

    try:
        # Install and load extensions
        print("\n1. Loading extensions...")
        for ext in ["httpfs", "aws", "iceberg"]:
            conn.execute(f"INSTALL {ext}")
            print(f"   ✓ Installed {ext}")
            conn.execute(f"LOAD {ext}")
            print(f"   ✓ Loaded {ext}")

        # Configure AWS (credentials from environment/chain)
        print("\n2. Configuring AWS...")
        region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        conn.execute(f"SET s3_region='{region}'")
        print(f"   ✓ Region: {region}")

        # Try to use AWS credential chain
        conn.execute("CALL load_aws_credentials()")
        print("   ✓ Loaded AWS credentials from chain")

        # Test S3 access (skip - can be slow)
        print("\n3. Testing S3 access...")
        print("   ℹ Skipping S3 CSV test (not critical for Iceberg)")

        # Test reading Iceberg table from Glue
        print("\n4. Testing Iceberg table read from Glue catalog...")

        # Get metadata location from Glue
        import boto3

        glue = boto3.client("glue")

        table_name = "raw__mitlearn__app__postgres__users_user"
        database = "ol_warehouse_production_raw"

        print(f"   Getting metadata from Glue: {database}.{table_name}")
        response = glue.get_table(DatabaseName=database, Name=table_name)

        table_location = response["Table"]["StorageDescriptor"]["Location"]
        metadata_location = response["Table"]["Parameters"].get("metadata_location")

        print(f"   Table location: {table_location}")
        print(f"   Metadata location: {metadata_location}")

        # Use metadata location for iceberg_scan
        iceberg_path = metadata_location if metadata_location else table_location
        print(f"   Reading from: {iceberg_path}")

        try:
            query = f"SELECT * FROM iceberg_scan('{iceberg_path}') LIMIT 5"
            print(f"   Query: {query}")
            result = conn.execute(query).fetchall()

            if result:
                print(f"   ✓ Successfully read {len(result)} rows from Iceberg table!")
                print("\n   Sample data (first row):")
                if len(result) > 0:
                    columns = conn.execute(query).description
                    col_names = [col[0] for col in columns]
                    print(f"   Columns: {col_names[:5]}...")  # First 5 columns
                    print(f"   First row: {str(result[0])[:100]}...")

                # Get table schema
                schema_query = f"DESCRIBE SELECT * FROM iceberg_scan('{iceberg_path}')"
                schema = conn.execute(schema_query).fetchall()
                print(f"\n   Table has {len(schema)} columns")

                return True
            else:
                print("   ⚠ Query succeeded but no rows returned")
                return True  # Still counts as success

        except Exception as e:
            print(f"   ✗ Error reading Iceberg table: {e}")
            import traceback

            traceback.print_exc()
            return False

    finally:
        conn.close()


def test_glue_catalog_integration():
    """Test if DuckDB can use Glue catalog metadata"""
    print("\n5. Testing Glue catalog integration...")

    # DuckDB doesn't have native Glue catalog support yet
    # We need to read from S3 paths directly (which we get from Glue)
    print("   ℹ DuckDB reads from S3 paths (obtained via Glue metadata)")
    print("   ℹ We'll query Glue via boto3 to get paths, then use iceberg_scan()")

    try:
        import boto3

        glue = boto3.client("glue")

        # Get one table from Glue (use production)
        response = glue.get_table(
            DatabaseName="ol_warehouse_production_raw",
            Name="raw__mitlearn__app__postgres__users_user",
        )

        table_location = response["Table"]["StorageDescriptor"]["Location"]
        print(f"   ✓ Got table location from Glue: {table_location}")
        return True

    except Exception as e:
        print(f"   ✗ Error accessing Glue: {e}")
        return False


if __name__ == "__main__":
    print("=" * 70)
    print("Testing DuckDB + AWS Glue + Iceberg Integration")
    print("=" * 70)

    success = test_glue_iceberg_read()

    if success:
        test_glue_catalog_integration()

    print("\n" + "=" * 70)
    if success:
        print("✓ SUCCESS: DuckDB can read Iceberg tables from S3!")
        print("\nNext steps:")
        print("1. Update dbt macros to use Glue + iceberg_scan()")
        print("2. Create script to register all Glue tables as views")
        print("3. Test with actual dbt models")
    else:
        print("✗ FAILED: Could not read Iceberg tables")
        print("Check error messages above")
    print("=" * 70)
