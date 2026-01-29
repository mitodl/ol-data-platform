#!/usr/bin/env bash
# Export all Superset assets from specified instance

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Parse arguments
INSTANCE_NAME="${1:-superset-production}"
ASSETS_DIR="${2:-$REPO_ROOT/assets}"

echo "================================================"
echo "Exporting Superset Assets from $INSTANCE_NAME"
echo "================================================"
echo ""

# Set the instance (don't persist to avoid changing global config)
echo "Setting instance to $INSTANCE_NAME..."
sup instance use "$INSTANCE_NAME"

echo ""
echo "Step 1: Exporting all datasets (via pagination)..."
sup dataset pull "$ASSETS_DIR" --instance "$INSTANCE_NAME" --overwrite

echo ""
echo "Step 2: Exporting all charts (via pagination)..."
sup chart pull "$ASSETS_DIR" --instance "$INSTANCE_NAME" --overwrite

echo ""
echo "Step 3: Exporting all dashboards (via pagination)..."
sup dashboard pull "$ASSETS_DIR" --instance "$INSTANCE_NAME" --overwrite

echo ""
echo "================================================"
echo "Export Complete!"
echo "================================================"
echo ""
echo "Assets exported to: $ASSETS_DIR"
echo ""

# Show what was exported
echo "Summary:"
echo "  Datasets:   $(find "$ASSETS_DIR/datasets" -name "*.yaml" 2>/dev/null | wc -l)"
echo "  Charts:     $(find "$ASSETS_DIR/charts" -name "*.yaml" 2>/dev/null | wc -l)"
echo "  Dashboards: $(find "$ASSETS_DIR/dashboards" -name "*.yaml" 2>/dev/null | wc -l)"
echo "  Databases:  $(find "$ASSETS_DIR/databases" -name "*.yaml" 2>/dev/null | wc -l)"
echo ""

echo "Usage examples:"
echo "  Export from production: ./scripts/export_all.sh superset-production"
echo "  Export from QA:         ./scripts/export_all.sh superset-qa"
echo "  Custom output dir:      ./scripts/export_all.sh superset-qa /tmp/qa-assets"
echo ""
