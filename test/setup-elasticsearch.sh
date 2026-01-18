#!/usr/bin/env bash
set -e

BASE_DIR=$(dirname "$(realpath "$0")")

# Wait for Elasticsearch to become ready.
max_attempts=30
attempt=0
until curl -s -u elastic:test http://localhost:9200/_cluster/health | grep -qE "green|yellow"; do
  attempt=$((attempt + 1))
  if [ $attempt -gt $max_attempts ]; then
    echo "Elasticsearch failed to become ready after $max_attempts attempts"
    exit 1
  fi
  echo "Waiting for Elasticsearch to be ready"
  sleep 2
done
echo "Elasticsearch is ready"

# Delete test index if exists.
echo "Deleting test index if exists"
curl -fs -X DELETE -u elastic:test http://localhost:9200/test && echo || true

# Create test index.
echo "Creating test index"
curl --fail-with-body -s -X PUT -u elastic:test http://localhost:9200/test \
  -H "Content-Type: application/json" \
  --data @"${BASE_DIR}/index-config.json" && echo

# Load sample data.
echo "Loading sample data"
curl --fail-with-body -s -X POST -u elastic:test http://localhost:9200/_bulk \
  -H "Content-Type: application/x-ndjson" \
  --data-binary @"${BASE_DIR}/sample-data.jsonl" && echo

# Refresh index.
echo "Refreshing test index"
curl --fail-with-body -s -X POST -u elastic:test http://localhost:9200/test/_refresh && echo

echo "Setup complete"
