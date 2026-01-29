#!/usr/bin/env bash
# Promote Superset assets from QA to production
# This script handles the deployment of QA-tested assets to production

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ASSETS_DIR="$REPO_ROOT/assets"

echo "================================================"
echo "Promote Superset Assets: QA → Production"
echo "================================================"
echo ""
echo "⚠️  WARNING: This will deploy assets to PRODUCTION"
echo ""

# Check if assets exist
if [ ! -d "$ASSETS_DIR" ]; then
    echo "❌ Error: Assets directory not found: $ASSETS_DIR"
    echo "Run './scripts/export_from_qa.sh' first to export assets from QA"
    exit 1
fi

# Check if there are uncommitted changes
if ! git diff --quiet HEAD -- "$ASSETS_DIR" 2>/dev/null; then
    echo "⚠️  Warning: You have uncommitted changes in assets/"
    echo ""
    git status "$ASSETS_DIR"
    echo ""
    echo "It's recommended to commit changes before promoting to production."
    read -p "Continue anyway? (yes/no): " -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
        echo "Promotion cancelled. Please commit your changes first."
        exit 0
    fi
fi

# Validate assets first
echo "Step 1: Validating assets..."
echo ""
if ! "$SCRIPT_DIR/validate_assets.sh"; then
    echo ""
    echo "❌ Validation failed. Fix errors before promoting to production."
    exit 1
fi

# Show what will be promoted
echo ""
echo "Step 2: Assets to promote:"
published_dashboards=$(find "$ASSETS_DIR/dashboards" -name "*.yaml" ! -name "untitled_*" | wc -l)
total_dashboards=$(find "$ASSETS_DIR/dashboards" -name "*.yaml" | wc -l)
charts=$(find "$ASSETS_DIR/charts" -name "*.yaml" | wc -l)
datasets=$(find "$ASSETS_DIR/datasets/Trino" -name "*.yaml" 2>/dev/null | wc -l)

echo "  Published Dashboards: $published_dashboards (of $total_dashboards total)"
echo "  Charts: $charts"
echo "  Datasets: $datasets"
echo ""

# Show recent git log for context
if git log -1 --oneline "$ASSETS_DIR" 2>/dev/null; then
    echo ""
    echo "Last commit affecting assets:"
    git log -1 --pretty=format:"  %h - %s (%cr by %an)" "$ASSETS_DIR"
    echo ""
    echo ""
fi

# Final confirmation
echo "⚠️  FINAL CONFIRMATION: Deploy these assets to PRODUCTION?"
echo ""
read -p "Type 'PROMOTE' to continue: " -r
echo ""

if [[ "$REPLY" != "PROMOTE" ]]; then
    echo "Promotion cancelled."
    exit 0
fi

# Set target instance to production
echo "Step 3: Setting instance to superset-production..."
sup instance use superset-production

echo ""
echo "Step 4: Promoting assets to production..."
echo ""

# Push charts with dependencies first
echo "Step 4a: Pushing charts (includes datasets and databases)..."
if [ -d "$ASSETS_DIR/charts" ] && [ $charts -gt 0 ]; then
    if sup chart push "$ASSETS_DIR" \
        --auto-map-databases \
        --overwrite \
        --continue-on-error \
        --force; then
        echo "✅ Charts promoted successfully"
    else
        echo "⚠️  Some charts may have failed - check logs above"
    fi
else
    echo "No charts to promote"
fi

echo ""
echo "Step 4b: Pushing published dashboards (includes all dependencies)..."

# Create temporary directory for published dashboards only
temp_dir=$(mktemp -d)
trap "rm -rf $temp_dir" EXIT

mkdir -p "$temp_dir/dashboards"

# Copy only published dashboards (exclude untitled)
published_count=0
for dashboard in "$ASSETS_DIR/dashboards"/*.yaml; do
    [ -f "$dashboard" ] || continue
    basename_file=$(basename "$dashboard")
    if [[ ! "$basename_file" =~ ^untitled_ ]]; then
        cp "$dashboard" "$temp_dir/dashboards/"
        ((published_count++))
    fi
done

if [ $published_count -gt 0 ]; then
    echo "Pushing $published_count published dashboards to production..."

    # Use sup dashboard push - handles all dependencies automatically
    if sup dashboard push "$temp_dir" \
        --auto-map-databases \
        --overwrite \
        --continue-on-error \
        --force; then
        echo "✅ Dashboards promoted successfully"
    else
        echo "❌ Dashboard promotion failed - see errors above"
        echo ""
        read -p "Continue anyway? (yes/no): " -r
        if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
            echo "Promotion aborted"
            sup instance use superset-qa
            exit 1
        fi
    fi
else
    echo "No published dashboards to promote"
fi

# Create a manifest of dashboards to promote
manifest_file="$REPO_ROOT/promotion_manifest.txt"
echo "Creating promotion manifest: $manifest_file"
echo ""
echo "# Superset Assets Promotion Manifest" > "$manifest_file"
echo "# Generated: $(date)" >> "$manifest_file"
echo "# Source: QA (bi-qa.ol.mit.edu)" >> "$manifest_file"
echo "# Target: Production (bi.ol.mit.edu)" >> "$manifest_file"
echo "" >> "$manifest_file"
echo "## Published Dashboards to Import" >> "$manifest_file"

for dashboard in "$ASSETS_DIR/dashboards"/*.yaml; do
    basename_file=$(basename "$dashboard")
    if [[ ! "$basename_file" =~ ^untitled_ ]]; then
        # Extract dashboard title from YAML
        title=$(grep "^dashboard_title:" "$dashboard" | sed 's/dashboard_title: //' | tr -d '"')
        echo "- $title (file: $basename_file)" >> "$manifest_file"
    fi
done

echo ""
echo "================================================"
echo "Promotion Complete!"
echo "================================================"
echo ""
echo "Next steps:"
echo "  1. Verify dashboards at: https://bi.ol.mit.edu/dashboard/list/"
echo "  2. Test dashboard functionality with production data"
echo "  3. Monitor for any errors or issues"
echo "  4. Update team on deployed changes"
echo ""
echo "Promotion manifest saved to: $manifest_file"
echo ""

# Restore to QA instance
echo "Restoring instance to superset-qa..."
sup instance use superset-qa

echo ""
echo "✅ Promotion complete! All assets deployed to production."
echo ""
