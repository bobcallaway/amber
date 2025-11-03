#!/bin/bash
#
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

# Configuration
PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-bcallaway-testing}"
NUM_ENTRIES="${NUM_ENTRIES:-100}"
TRILLIAN_ADDR="${TRILLIAN_ADDR:-localhost}"
TRILLIAN_PORT="${TRILLIAN_PORT:-8090}"
AMBER_SERVER_PORT="${AMBER_SERVER_PORT:-8093}"

# Configure emulators
# Note: Using real GCS now; do not set STORAGE_EMULATOR_HOST
export SPANNER_EMULATOR_HOST="${SPANNER_EMULATOR_HOST:-localhost:9010}"

echo "================================================"
echo "amber-test - E2E Testing for amber-polymerize"
echo "================================================"
echo ""

# Check if docker compose is running
if ! docker compose ps | grep -q "trillian-log-server.*Up"; then
    echo "❌ Error: trillian-log-server is not running"
    echo "   Please start it with: docker compose up -d"
    exit 1
fi

# Check for project ID
if [ -z "$PROJECT_ID" ]; then
    echo "❌ Error: GOOGLE_CLOUD_PROJECT environment variable is not set"
    echo "   Please set it with: export GOOGLE_CLOUD_PROJECT=your-project-id"
    exit 1
fi

echo "Configuration:"
echo "  Project ID: $PROJECT_ID"
echo "  Entries: $NUM_ENTRIES"
echo "  Trillian: $TRILLIAN_ADDR:$TRILLIAN_PORT"
echo "  Amber Server Port: $AMBER_SERVER_PORT"
echo "  Storage: Google Cloud Storage (project: $PROJECT_ID)"
echo "  Spanner Emulator: $SPANNER_EMULATOR_HOST"
echo ""

# Build the test tool if needed
if [ ! -f "./amber-test" ] || [ "./cmd/amber-test/app/test.go" -nt "./amber-test" ]; then
    echo "🔨 Building amber-test..."
    go build -o amber-test ./cmd/amber-test
    echo ""
fi

# Run the test
echo "🚀 Starting test..."
echo ""

./amber-test test \
    --project-id="$PROJECT_ID" \
    --num-entries="$NUM_ENTRIES" \
    --trillian-address="$TRILLIAN_ADDR" \
    --trillian-port="$TRILLIAN_PORT" \
    --amber-server-port="$AMBER_SERVER_PORT" \
    "$@"

echo ""
echo "================================================"
echo "✅ Test completed successfully!"
echo "================================================"
