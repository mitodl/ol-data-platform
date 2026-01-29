#!/usr/bin/env bash
# Export Superset assets from QA instance for promotion to production

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ASSETS_DIR="$REPO_ROOT/assets"

echo "================================================"
echo "Exporting Superset Assets from QA"
echo "================================================"
echo ""
echo "This will export all assets from the QA instance to prepare"
echo "them for promotion to production."
echo ""

# Ensure we're using QA instance
echo "Setting instance to superset-qa..."
sup instance use superset-qa

echo ""
echo "Exporting dashboards (with dependencies: charts, datasets, databases)..."
echo "This will OVERWRITE the existing assets in: $ASSETS_DIR"
echo ""
read -p "Continue? (yes/no): " -r
echo ""

if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
    echo "Export cancelled."
    exit 0
fi

sup dashboard pull "$ASSETS_DIR" --overwrite

echo ""
echo "================================================"
echo "Export from QA Complete!"
echo "================================================"
echo ""
echo "Assets exported to: $ASSETS_DIR"
echo ""
echo "Next steps:"
echo "  1. Review changes: git status && git diff assets/"
echo "  2. Validate: ./scripts/validate_assets.sh"
echo "  3. Commit: git add assets/ && git commit -m 'Update from QA - <description>'"
echo "  4. Promote to production: ./scripts/promote_to_production.sh"
echo ""
