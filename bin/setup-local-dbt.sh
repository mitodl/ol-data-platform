#!/usr/bin/env bash
set -euo pipefail

# setup-local-dbt.sh
# Setup script for local dbt development with DuckDB + Iceberg
# 
# This script configures a local development environment that:
# - Reads Iceberg tables directly from S3 (zero storage duplication)
# - Materializes only transformed models locally (minimal disk usage)
# - Eliminates Trino compute costs for development iteration
#
# Prerequisites:
# - AWS credentials configured (~/.aws/credentials or environment variables)
# - uv package manager installed
# - Network access to S3 and AWS Glue

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DUCKDB_DIR="${HOME}/.ol-dbt"
DUCKDB_PATH="${DUCKDB_DIR}/local.duckdb"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

log_success() {
    echo -e "${GREEN}✓${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

log_error() {
    echo -e "${RED}✗${NC} $1"
}

log_section() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

# Check prerequisites
check_prerequisites() {
    log_section "Checking Prerequisites"
    
    local all_good=true
    
    # Check uv
    if command -v uv &> /dev/null; then
        local uv_version=$(uv --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' || echo "unknown")
        log_success "uv is installed (version: ${uv_version})"
    else
        log_error "uv is not installed. Install from: https://github.com/astral-sh/uv"
        all_good=false
    fi
    
    # Check AWS credentials
    if aws sts get-caller-identity &> /dev/null; then
        local aws_account=$(aws sts get-caller-identity --query Account --output text)
        local aws_user=$(aws sts get-caller-identity --query Arn --output text | grep -oE '[^/]+$')
        log_success "AWS credentials configured (Account: ${aws_account}, User: ${aws_user})"
    else
        log_error "AWS credentials not configured. Run 'aws configure' or set AWS_* environment variables"
        all_good=false
    fi
    
    # Check AWS Glue access
    if aws glue get-databases --max-results 1 &> /dev/null; then
        log_success "AWS Glue access verified"
    else
        log_warning "Cannot access AWS Glue. You may not have sufficient IAM permissions"
        log_info "Required permissions: glue:GetDatabase, glue:GetTable, glue:GetTables"
    fi
    
    # Check S3 access to production bucket
    if aws s3 ls s3://ol-data-lake-raw-production/ --max-items 1 &> /dev/null 2>&1; then
        log_success "S3 production bucket access verified"
    else
        log_warning "Cannot access s3://ol-data-lake-raw-production/. Iceberg reads will fail."
    fi
    
    if [ "$all_good" = false ]; then
        log_error "Prerequisites check failed. Please fix the issues above and try again."
        exit 1
    fi
}

# Create DuckDB directory
setup_duckdb_directory() {
    log_section "Setting Up DuckDB Directory"
    
    if [ -d "$DUCKDB_DIR" ]; then
        log_info "Directory already exists: $DUCKDB_DIR"
    else
        log_info "Creating directory: $DUCKDB_DIR"
        mkdir -p "$DUCKDB_DIR"
        log_success "Created $DUCKDB_DIR"
    fi
    
    # Create temp directory
    if [ ! -d "${DUCKDB_DIR}/temp" ]; then
        mkdir -p "${DUCKDB_DIR}/temp"
        log_success "Created temp directory"
    fi
}

# Initialize DuckDB database
initialize_duckdb() {
    log_section "Initializing DuckDB Database"
    
    if [ -f "$DUCKDB_PATH" ]; then
        local size=$(du -h "$DUCKDB_PATH" | cut -f1)
        log_info "DuckDB database already exists (size: ${size})"
        
        read -p "Do you want to recreate it? This will delete all local data [y/N]: " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            log_info "Removing existing database..."
            rm "$DUCKDB_PATH"
        else
            log_info "Keeping existing database"
            return 0
        fi
    fi
    
    log_info "Creating new DuckDB database with extensions..."
    
    uv run python << EOF
import duckdb

conn = duckdb.connect('${DUCKDB_PATH}')

# Install and load extensions
print("  Installing httpfs extension...")
conn.execute("INSTALL httpfs")
conn.execute("LOAD httpfs")

print("  Installing aws extension...")
conn.execute("INSTALL aws")
conn.execute("LOAD aws")

print("  Installing iceberg extension...")
conn.execute("INSTALL iceberg")
conn.execute("LOAD iceberg")

print("  Loading AWS credentials...")
conn.execute("CALL load_aws_credentials()")

# Create registry table for tracking registered sources
print("  Creating registry table...")
conn.execute("""
    CREATE TABLE IF NOT EXISTS _glue_source_registry (
        database_name VARCHAR,
        table_name VARCHAR,
        view_name VARCHAR,
        metadata_location VARCHAR,
        registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (database_name, table_name)
    )
""")

conn.close()
print("✓ DuckDB initialized successfully")
EOF
    
    log_success "DuckDB database created at $DUCKDB_PATH"
}

# Install dbt dependencies
install_dbt_deps() {
    log_section "Installing dbt Dependencies"
    
    cd "${PROJECT_ROOT}/src/ol_dbt"
    
    log_info "Running dbt deps..."
    uv run dbt deps --quiet
    
    log_success "dbt dependencies installed"
}

# Register Iceberg sources
register_iceberg_sources() {
    log_section "Registering Iceberg Sources from AWS Glue"
    
    log_info "This will register all Iceberg tables from production as DuckDB views"
    log_info "Note: This only registers metadata - no data is copied locally"
    log_info "Estimated time: 10-20 minutes for all layers"
    echo ""
    
    read -p "Register which layers? [all/raw/staging]: " -r
    echo
    
    local layers_flag=""
    case $REPLY in
        all|ALL)
            layers_flag="--all-layers"
            ;;
        raw|RAW)
            layers_flag="--database ol_warehouse_production_raw"
            ;;
        staging|STAGING)
            layers_flag="--database ol_warehouse_production_staging"
            ;;
        *)
            log_info "Defaulting to raw layer only"
            layers_flag="--database ol_warehouse_production_raw"
            ;;
    esac
    
    log_info "Starting registration..."
    cd "$PROJECT_ROOT"
    
    if uv run python bin/register-glue-sources.py $layers_flag; then
        log_success "Iceberg sources registered successfully"
        
        # Show statistics
        uv run python << EOF
import duckdb
conn = duckdb.connect('${DUCKDB_PATH}')
result = conn.execute("""
    SELECT 
        database_name,
        COUNT(*) as table_count
    FROM _glue_source_registry
    GROUP BY database_name
    ORDER BY database_name
""").fetchall()

print("\n  Registered tables by layer:")
for db, count in result:
    layer = db.replace('ol_warehouse_production_', '')
    print(f"    • {layer}: {count:,} tables")

total = sum(count for _, count in result)
print(f"\n  Total: {total:,} Iceberg views registered")
conn.close()
EOF
    else
        log_error "Registration failed. Check error messages above."
        return 1
    fi
}

# Test the setup
test_setup() {
    log_section "Testing Setup"
    
    log_info "Testing DuckDB connection and Iceberg access..."
    
    cd "$PROJECT_ROOT"
    if uv run python bin/test-glue-iceberg.py > /dev/null 2>&1; then
        log_success "Iceberg connectivity test passed"
    else
        log_warning "Iceberg connectivity test failed (non-critical)"
    fi
    
    log_info "Testing dbt compilation..."
    cd "${PROJECT_ROOT}/src/ol_dbt"
    
    if uv run dbt compile --select stg__mitlearn__app__postgres__users_user --target dev_local > /dev/null 2>&1; then
        log_success "dbt compilation test passed"
    else
        log_error "dbt compilation test failed"
        return 1
    fi
}

# Show next steps
show_next_steps() {
    log_section "Setup Complete! 🎉"
    
    echo ""
    log_success "Local dbt development environment is ready"
    echo ""
    echo "📁 DuckDB database: ${DUCKDB_PATH}"
    echo "📊 Local storage: $(du -sh $DUCKDB_DIR | cut -f1)"
    echo ""
    echo -e "${GREEN}Next steps:${NC}"
    echo ""
    echo "  1. Navigate to dbt project:"
    echo -e "     ${BLUE}cd ${PROJECT_ROOT}/src/ol_dbt${NC}"
    echo ""
    echo "  2. Run a model locally:"
    echo -e "     ${BLUE}uv run dbt run --select stg__mitlearn__app__postgres__users_user --target dev_local${NC}"
    echo ""
    echo "  3. Build multiple models:"
    echo -e "     ${BLUE}uv run dbt run --select tag:mitlearn --target dev_local${NC}"
    echo ""
    echo "  4. Test your changes:"
    echo -e "     ${BLUE}uv run dbt test --select stg__mitlearn__app__postgres__users_user --target dev_local${NC}"
    echo ""
    echo -e "${YELLOW}Important notes:${NC}"
    echo "  • Raw data is read directly from S3 (zero local duplication)"
    echo "  • Only transformed models are stored locally"
    echo "  • Trino-specific functions use adapter dispatch for compatibility"
    echo "  • Use --target dev_local for local, production/qa for Trino"
    echo ""
    echo "📚 Documentation: See ${PROJECT_ROOT}/docs/ for more details"
    echo ""
}

# Main execution
main() {
    echo ""
    echo -e "${BLUE}╔═══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║  Local dbt Development Environment Setup                     ║${NC}"
    echo -e "${BLUE}║  DuckDB + Iceberg Direct Read from S3                        ║${NC}"
    echo -e "${BLUE}╚═══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    
    check_prerequisites
    setup_duckdb_directory
    initialize_duckdb
    install_dbt_deps
    register_iceberg_sources
    test_setup
    show_next_steps
}

# Run main function
main
