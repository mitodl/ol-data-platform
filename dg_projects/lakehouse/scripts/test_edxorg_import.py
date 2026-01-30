#!/usr/bin/env python3
"""
Quick test script to verify the EdX.org S3 dlt pipeline loads module.
"""

import sys
from pathlib import Path

# Add lakehouse to path
lakehouse_root = Path(__file__).parent.parent
sys.path.insert(0, str(lakehouse_root))

print("Testing EdX.org S3 loads module import...")

try:
    from lakehouse.defs.edxorg_s3_ingestion import loads
    print("✓ Module imported successfully")
    
    print("\nAvailable objects:")
    print(f"  - edxorg_s3_source: {loads.edxorg_s3_source}")
    print(f"  - edxorg_s3_pipeline: {loads.edxorg_s3_pipeline}")
    print(f"  - destination_env: {loads.destination_env}")
    
    print("\nPipeline configuration:")
    print(f"  - Name: {loads.edxorg_s3_pipeline.pipeline_name}")
    print(f"  - Destination: {loads.edxorg_s3_pipeline.destination}")
    print(f"  - Dataset: {loads.edxorg_s3_pipeline.dataset_name}")
    
    print("\n✓ All checks passed!")
    
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
