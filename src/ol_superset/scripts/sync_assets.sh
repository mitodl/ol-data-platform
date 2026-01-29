#!/usr/bin/env bash
# Sync Superset assets from one environment to another
# Handles database UUID mapping automatically

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ASSETS_DIR="$REPO_ROOT/assets"

# Parse arguments
SOURCE_INSTANCE="${1:-}"
TARGET_INSTANCE="${2:-}"

if [ -z "$SOURCE_INSTANCE" ] || [ -z "$TARGET_INSTANCE" ]; then
    echo "Usage: $0 <source-instance> <target-instance>"
    echo ""
    echo "Examples:"
    echo "  $0 superset-production superset-qa"
    echo "  $0 superset-qa superset-production"
    echo ""
    echo "Available instances:"
    sup instance list 2>/dev/null | grep -E "^\s+\w+" || echo "  (Run 'sup instance list' to see configured instances)"
    exit 1
fi

echo "================================================"
echo "Syncing Superset Assets"
echo "================================================"
echo ""
echo "Source: $SOURCE_INSTANCE"
echo "Target: $TARGET_INSTANCE"
echo ""

# Check if assets exist
if [ ! -d "$ASSETS_DIR" ]; then
    echo "❌ Error: Assets directory not found: $ASSETS_DIR"
    echo "Run './scripts/export_all.sh $SOURCE_INSTANCE' first to export assets"
    exit 1
fi

# Show what will be synced
echo "Assets to sync:"
echo "  Dashboards: $(find "$ASSETS_DIR/dashboards" -name "*.yaml" ! -name "untitled_*" 2>/dev/null | wc -l)"
echo "  Charts:     $(find "$ASSETS_DIR/charts" -name "*.yaml" 2>/dev/null | wc -l)"
echo "  Datasets:   $(find "$ASSETS_DIR/datasets" -name "*.yaml" 2>/dev/null | wc -l)"
echo ""
echo "⚠️  This will overwrite existing assets in $TARGET_INSTANCE if they share the same UUID"
echo ""
read -p "Proceed with sync? (yes/no): " -r
echo ""

if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
    echo "Sync cancelled."
    exit 0
fi

# Step 1: Map database UUIDs from source to target
echo ""
echo "Step 1: Mapping database UUIDs for target environment..."
python3 "$SCRIPT_DIR/map_database_uuids.py" "$TARGET_INSTANCE"
if [ $? -ne 0 ]; then
    echo "❌ Error: Failed to map database UUIDs"
    exit 1
fi

# Step 2: Push datasets
echo ""
echo "Step 2: Syncing datasets..."
if [ -d "$ASSETS_DIR/datasets" ]; then
    dataset_count=$(find "$ASSETS_DIR/datasets" -name "*.yaml" 2>/dev/null | wc -l)
    echo "Pushing $dataset_count datasets..."

    if sup dataset push "$ASSETS_DIR" --instance "$TARGET_INSTANCE" --overwrite --force --continue-on-error 2>&1 | grep -v "^DEBUG:"; then
        echo "✅ Datasets synced"
    else
        echo "⚠️  Some datasets failed (non-critical, check logs)"
    fi
else
    echo "No datasets found"
fi

# Step 3: Push charts
echo ""
echo "Step 3: Syncing charts..."
if [ -d "$ASSETS_DIR/charts" ]; then
    chart_count=$(find "$ASSETS_DIR/charts" -name "*.yaml" 2>/dev/null | wc -l)
    echo "Pushing $chart_count charts..."

    if sup chart push "$ASSETS_DIR" --instance "$TARGET_INSTANCE" --overwrite --force --continue-on-error 2>&1 | grep -v "^DEBUG:"; then
        echo "✅ Charts synced"
    else
        echo "⚠️  Some charts failed (non-critical, check logs)"
    fi
else
    echo "No charts found"
fi

# Step 4: Push dashboards
echo ""
echo "Step 4: Syncing dashboards..."
if [ -d "$ASSETS_DIR/dashboards" ]; then
    # Count published dashboards
    published_count=$(find "$ASSETS_DIR/dashboards" -name "*.yaml" ! -name "untitled_*" 2>/dev/null | wc -l)

    if [ $published_count -gt 0 ]; then
        echo "Pushing $published_count published dashboards..."

        if sup dashboard push "$ASSETS_DIR" --instance "$TARGET_INSTANCE" --overwrite --force --continue-on-error 2>&1 | grep -v "^DEBUG:"; then
            echo "✅ Dashboards synced"
        else
            echo "⚠️  Some dashboards failed (non-critical, check logs)"
        fi
    else
        echo "No published dashboards to sync (all are untitled)"
    fi
else
    echo "No dashboards found"
fi

echo ""
echo "================================================"
echo "Sync Complete!"
echo "================================================"
echo ""
echo "Verify at:"
if [ "$TARGET_INSTANCE" = "superset-qa" ]; then
    echo "  • Dashboards: https://bi-qa.ol.mit.edu/dashboard/list/"
    echo "  • Charts:     https://bi-qa.ol.mit.edu/chart/list/"
    echo "  • Datasets:   https://bi-qa.ol.mit.edu/tablemodelview/list/"
elif [ "$TARGET_INSTANCE" = "superset-production" ]; then
    echo "  • Dashboards: https://bi.ol.mit.edu/dashboard/list/"
    echo "  • Charts:     https://bi.ol.mit.edu/chart/list/"
    echo "  • Datasets:   https://bi.ol.mit.edu/tablemodelview/list/"
else
    echo "  • Check your Superset instance for imported assets"
fi
echo ""
