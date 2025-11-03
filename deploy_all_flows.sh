#!/bin/bash

# Script to deploy all flows in the flows directory
# This script finds all Python files in the flows directory and deploys them

set -e

PROJECT_PATH="/opt/prefect"

echo "Searching for flows in flows/ directory..."

# Find all Python files in flows directory (excluding __init__.py)
FLOW_FILES=$(find flows -type f -name "*.py" ! -name "__init__.py")

if [ -z "$FLOW_FILES" ]; then
    echo "No flow files found in flows/ directory"
    exit 0
fi

echo "Found flows:"
echo "$FLOW_FILES"
echo ""

# Counter for deployment summary
TOTAL=0
SUCCESS=0
FAILED=0

# Deploy each flow
while IFS= read -r flow_file; do
    TOTAL=$((TOTAL + 1))
    echo "[$TOTAL] Deploying: $flow_file"

    if docker exec -it prefect-client python $PROJECT_PATH/$flow_file; then
        echo "  ✓ Successfully deployed: $flow_file"
        SUCCESS=$((SUCCESS + 1))
    else
        echo "  ✗ Error deploying: $flow_file"
        FAILED=$((FAILED + 1))
    fi

    echo ""
done <<< "$FLOW_FILES"

# Print summary
echo "========================================="
echo "Deployment Summary:"
echo "  Total flows: $TOTAL"
echo "  Successful: $SUCCESS"
echo "  Failed: $FAILED"
echo "========================================="

if [ $FAILED -gt 0 ]; then
    exit 1
fi