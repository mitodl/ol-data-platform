#!/usr/bin/env bash
# Validate Superset asset definitions

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ASSETS_DIR="$REPO_ROOT/assets"

echo "================================================"
echo "Validating Superset Assets"
echo "================================================"
echo ""

if [ ! -d "$ASSETS_DIR" ]; then
    echo "❌ Error: Assets directory not found: $ASSETS_DIR"
    exit 1
fi

# Check sync config exists (informational only)
if [ -f "$REPO_ROOT/sync_config.yml" ]; then
    echo "✅ sync_config.yml found (documentation/reference)"
    echo ""
fi

# Count assets by type
echo "Asset inventory:"
dashboard_count=$(find "$ASSETS_DIR/dashboards" -name "*.yaml" 2>/dev/null | wc -l)
published_count=$(find "$ASSETS_DIR/dashboards" -name "*.yaml" ! -name "untitled_*" 2>/dev/null | wc -l)
chart_count=$(find "$ASSETS_DIR/charts" -name "*.yaml" 2>/dev/null | wc -l)
dataset_count=$(find "$ASSETS_DIR/datasets" -name "*.yaml" 2>/dev/null | wc -l)
database_count=$(find "$ASSETS_DIR/databases" -name "*.yaml" 2>/dev/null | wc -l)

echo "  Dashboards: $dashboard_count ($published_count published, $((dashboard_count - published_count)) drafts)"
echo "  Charts:     $chart_count"
echo "  Datasets:   $dataset_count"
echo "  Databases:  $database_count"
echo ""

# Basic YAML syntax validation using Python
echo "Checking YAML syntax..."

python3 << 'PYEOF'
import sys
import yaml
from pathlib import Path

assets_dir = Path("assets")
errors = []
checked = 0

for yaml_file in sorted(assets_dir.rglob("*.yaml")):
    try:
        with open(yaml_file) as f:
            yaml.safe_load(f)
        checked += 1
    except Exception as e:
        errors.append((yaml_file, str(e)))

if errors:
    print(f"❌ Found {len(errors)} invalid YAML file(s) out of {checked + len(errors)} total:")
    for file, error in errors:
        print(f"   {file}: {error}")
    sys.exit(1)
else:
    print(f"✅ All {checked} YAML files are syntactically valid")
PYEOF

if [ $? -ne 0 ]; then
    exit 1
fi

echo ""

# Check for common issues
echo "Checking for common issues..."
warnings=0

# Check for database passwords (shouldn't be present)
if grep -r "password" "$ASSETS_DIR/databases"/*.yaml 2>/dev/null | grep -v "^#" >/dev/null 2>&1; then
    echo "⚠️  Warning: Database passwords found in exported configs"
    ((warnings++))
fi

# Check for production-specific references that might need transformation
prod_refs=$(grep -r "ol_data_lake_production\|ol_warehouse_production" "$ASSETS_DIR" 2>/dev/null | wc -l)
if [ $prod_refs -gt 0 ]; then
    echo "ℹ️  Info: Found $prod_refs references to 'production' catalogs/schemas"
    echo "   These will need transformation when deploying to QA"
fi

if [ $warnings -eq 0 ]; then
    echo "✅ No security issues detected"
fi

echo ""
echo "================================================"
echo "Validation Complete!"
echo "================================================"
echo ""
echo "Summary:"
echo "  ✅ Exported: $dashboard_count dashboards, $chart_count charts, $dataset_count datasets"
echo "  ✅ All YAML files are valid"
echo "  ✅ Ready for version control"
echo ""
