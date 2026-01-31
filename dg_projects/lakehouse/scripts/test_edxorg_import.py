#!/usr/bin/env python3
"""Quick test script to verify the EdX.org S3 dlt pipeline loads module."""

import sys
from pathlib import Path

# Add lakehouse to path
lakehouse_root = Path(__file__).parent.parent
sys.path.insert(0, str(lakehouse_root))

print("Testing EdX.org S3 loads module import...")  # noqa: T201

try:
    from lakehouse.defs.edxorg_s3_ingestion import loads

    print("✓ Module imported successfully")  # noqa: T201

    print("\nAvailable objects:")  # noqa: T201
    print(f"  - edxorg_s3_source: {loads.edxorg_s3_source}")  # noqa: T201
    print(f"  - edxorg_s3_pipeline: {loads.edxorg_s3_pipeline}")  # noqa: T201
    print(f"  - destination_env: {loads.destination_env}")  # noqa: T201

    print("\nPipeline configuration:")  # noqa: T201
    print(f"  - Name: {loads.edxorg_s3_pipeline.pipeline_name}")  # noqa: T201
    print(f"  - Destination: {loads.edxorg_s3_pipeline.destination}")  # noqa: T201
    print(f"  - Dataset: {loads.edxorg_s3_pipeline.dataset_name}")  # noqa: T201

    print("\n✓ All checks passed!")  # noqa: T201

except Exception as e:  # noqa: BLE001
    print(f"✗ Error: {e}")  # noqa: T201
    import traceback

    traceback.print_exc()
    sys.exit(1)
