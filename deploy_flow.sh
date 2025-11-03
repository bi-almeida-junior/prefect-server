#!/bin/bash

# Script to deploy a specific flow to Prefect
# Usage: ./deploy_flow.sh flows/salesforce/salesforce_to_snowflake.py

set -e

FLOW_FILE=$1

if [ -z "$FLOW_FILE" ]; then
    echo "Usage: $0 <flow_file>"
    echo "Example: $0 flows/salesforce/salesforce_to_snowflake.py"
    exit 1
fi

if [ ! -f "$FLOW_FILE" ]; then
    echo "Error: Flow file '$FLOW_FILE' not found"
    exit 1
fi

echo "Deploying flow: $FLOW_FILE"

# Execute the flow in the prefect-client container
docker exec -it prefect-client python /opt/prefect/$FLOW_FILE

if [ $? -eq 0 ]; then
    echo "Successfully deployed: $FLOW_FILE"
else
    echo "Error deploying: $FLOW_FILE"
    exit 1
fi