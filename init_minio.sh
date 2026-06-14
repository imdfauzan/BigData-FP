#!/bin/bash

###############################################################################
# MinIO Bucket Initialization Script
# Storage & Lakehouse Architecture (Anggota 4)
# 
# Script ini dijalankan setelah MinIO container startup untuk:
# 1. Membuat buckets (bronze, silver, gold)
# 2. Set bucket policies
# 3. Verify struktur folder
#
# Run secara manual:
#   bash init_minio.sh
#
# Atau via docker-compose (perlu ditambah ke entrypoint MinIO service)
###############################################################################

set -e

# ============================================================================
# CONFIGURATION
# ============================================================================

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin123}"

# Buckets
BUCKETS=("bronze" "silver" "gold")

# Colors untuk output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# ============================================================================
# FUNCTIONS
# ============================================================================

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Install mc (MinIO client) jika belum ada
ensure_mc_installed() {
    if ! command -v mc &> /dev/null; then
        log_info "Installing MinIO Client (mc)..."
        curl https://dl.min.io/client/mc/release/linux-amd64/mc --create-dirs -o /usr/local/bin/mc
        chmod +x /usr/local/bin/mc
        log_info "MinIO Client installed"
    else
        log_info "MinIO Client (mc) sudah terinstall"
    fi
}

# Configure mc alias untuk MinIO
configure_mc_alias() {
    log_info "Configuring MinIO client alias..."
    
    # Remove existing alias jika ada
    mc alias remove minio 2>/dev/null || true
    
    # Add new alias
    mc alias set minio "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" --api S3v4
    
    log_info "MinIO client alias configured"
}

# Wait untuk MinIO ready
wait_for_minio() {
    log_info "Waiting for MinIO to be ready..."
    
    max_attempts=30
    attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if curl -f -s "$MINIO_ENDPOINT/minio/health/live" > /dev/null 2>&1; then
            log_info "MinIO is ready!"
            return 0
        fi
        
        attempt=$((attempt + 1))
        sleep 1
    done
    
    log_error "MinIO failed to start after $max_attempts attempts"
    return 1
}

# Create buckets
create_buckets() {
    log_info "Creating buckets..."
    
    for bucket in "${BUCKETS[@]}"; do
        if mc ls "minio/$bucket" > /dev/null 2>&1; then
            log_warn "Bucket '$bucket' already exists"
        else
            mc mb "minio/$bucket"
            log_info "Bucket '$bucket' created"
        fi
    done
}

# Set bucket policies untuk public read (opsional, untuk dashboard visualization)
set_bucket_policies() {
    log_info "Setting bucket policies..."
    
    # Define public policy untuk Gold bucket (untuk Superset/Dashboard read)
    GOLD_POLICY='{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::gold/*"
            },
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:ListBucket",
                "Resource": "arn:aws:s3:::gold"
            }
        ]
    }'
    
    # Apply policy ke Gold bucket (untuk public read-only access)
    echo "$GOLD_POLICY" > /tmp/gold_policy.json
    mc policy set /tmp/gold_policy.json minio/gold 2>/dev/null || true
    rm /tmp/gold_policy.json
    
    log_info "Bucket policies configured"
}

# Create folder structure di dalam buckets
create_folder_structure() {
    log_info "Creating folder structure..."
    
    # Bronze structure: violations/
    echo "" | mc pipe "minio/bronze/violations/.keep"
    log_info "Created folder: bronze/violations"
    
    # Silver structure: violations_clean/
    echo "" | mc pipe "minio/silver/violations_clean/.keep"
    log_info "Created folder: silver/violations_clean"
    
    # Gold structure: violations_agg/ dan aggregations/
    echo "" | mc pipe "minio/gold/violations_agg/.keep"
    echo "" | mc pipe "minio/gold/aggregations/.keep"
    log_info "Created folder: gold/violations_agg & aggregations"
}

# Verify setup
verify_setup() {
    log_info "Verifying setup..."
    
    for bucket in "${BUCKETS[@]}"; do
        if mc ls "minio/$bucket" > /dev/null 2>&1; then
            log_info "✓ Bucket '$bucket' verified"
        else
            log_error "✗ Bucket '$bucket' verification failed"
            return 1
        fi
    done
    
    log_info "✓ All buckets verified successfully"
}

# Display info
display_info() {
    log_info "======================================"
    log_info "MinIO Setup Complete!"
    log_info "======================================"
    log_info "Endpoint: $MINIO_ENDPOINT"
    log_info "Access Key: $MINIO_ACCESS_KEY"
    log_info "Secret Key: $MINIO_SECRET_KEY"
    log_info ""
    log_info "Buckets:"
    for bucket in "${BUCKETS[@]}"; do
        log_info "  - $bucket"
    done
    log_info ""
    log_info "MinIO Console: http://localhost:9001"
    log_info "======================================"
}

# ============================================================================
# MAIN
# ============================================================================

main() {
    log_info "Starting MinIO initialization..."
    
    ensure_mc_installed
    wait_for_minio
    configure_mc_alias
    create_buckets
    set_bucket_policies
    create_folder_structure
    verify_setup
    display_info
    
    log_info "✓ MinIO initialization completed successfully!"
}

main "$@"
