# amber-test

End-to-end testing tool for amber-polymerize migration process.

## Overview

This tool performs a comprehensive test of the amber-polymerize migration workflow:

1. **Creates a new Trillian log** via the Trillian Admin API
2. **Populates the log** with a configurable number of random entries
3. **Waits for integration** - ensures all entries are integrated into the Merkle tree
4. **Runs amber-polymerize** to migrate the Trillian log to a Tessera tile backend in GCS
5. **Starts amber-server** configured to serve the migrated log
6. **Validates migration** by comparing `GetLeavesByRange` responses between:
   - Original Trillian log server
   - amber-server serving the migrated Tessera tiles

## Prerequisites

- Docker Compose environment running with:
  - `trillian-log-server` (port 8090)
  - `trillian-log-signer`
  - `mysql`
  - `spanner-emulator` (ports 9010, 9020) - for local testing
- GCP project ID
- Go 1.24+ installed

## Usage

### Quick Start with Docker Compose and Emulators

Start the test infrastructure:

```bash
docker compose up -d
```

Run the test (uses emulators automatically):

```bash
export GOOGLE_CLOUD_PROJECT=test-project
./scripts/run-test.sh
```

Or run directly:

```bash
export GOOGLE_CLOUD_PROJECT=test-project
go run ./cmd/amber-test test \
  --project-id=test-project \
  --num-entries=100
```

The test automatically uses:
- **Fake GCS server** at `http://localhost:4443`
- **Spanner emulator** at `localhost:9010`

### Testing with Real GCP Services

To test against real GCS and Spanner:

```bash
# Disable emulators
unset STORAGE_EMULATOR_HOST
unset SPANNER_EMULATOR_HOST

# Authenticate
gcloud auth application-default login

# Run test
export GOOGLE_CLOUD_PROJECT=your-real-project
./scripts/run-test.sh
```

### Command Line Options

| Flag | Description | Default | Required |
|------|-------------|---------|----------|
| `--project-id` | GCP project ID for GCS access | - | ✅ |
| `--num-entries` | Number of random entries to add | 100 | ❌ |
| `--trillian-address` | Trillian log server address | localhost | ❌ |
| `--trillian-port` | Trillian log server port | 8090 | ❌ |
| `--bucket` | GCS bucket name (auto-generated if not provided) | auto | ❌ |
| `--origin` | Log origin/name | amber-test-log | ❌ |
| `--amber-server-address` | Amber server address | localhost | ❌ |
| `--amber-server-port` | Amber server port | 8093 | ❌ |
| `--batch-size` | Batch size for entry comparison | 10 | ❌ |

### Example with Custom Configuration

```bash
go run ./cmd/amber-test test \
  --project-id=my-gcp-project \
  --num-entries=1000 \
  --bucket=my-test-bucket \
  --origin=my-test-log \
  --batch-size=50 \
  --trillian-address=localhost \
  --trillian-port=8090 \
  --amber-server-port=8093
```

## What Gets Tested

The tool validates the following for each entry:

### Entry Data
- ✅ **Leaf Index** - Sequential index matches
- ✅ **Leaf Value** - Raw entry data is identical
- ✅ **Merkle Leaf Hash** - Hash computation is correct

### Timestamps
- ✅ **Queue Timestamp** - Time when entry was queued
- ✅ **Integrate Timestamp** - Time when entry was integrated into the tree

### Batch Testing
The tool compares entries in configurable batches to efficiently test large logs while providing granular error reporting.

## Test Workflow

```
┌─────────────────────────────────────────────────────────┐
│ 1. Create Trillian Log                                  │
│    - Via Trillian Admin API                             │
│    - Returns unique log ID                              │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│ 2. Populate Log with Entries                            │
│    - Generate N random entries                          │
│    - Queue each via QueueLeaf API                       │
│    - Entries contain: timestamp + counter + random data │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│ 3. Wait for Integration                                 │
│    - Poll GetLatestSignedLogRoot                        │
│    - Wait until TreeSize >= N entries                   │
│    - Timeout after 30 seconds                           │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│ 4. Run amber-polymerize                                 │
│    - Generate temporary config file                     │
│    - Build binary if needed                             │
│    - Migrate log to GCS bucket (Tessera tiles)          │
│    - Includes tile data, checkpoint, and hash shards    │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│ 5. Start amber-server                                   │
│    - Generate server config with log mapping            │
│    - Build binary if needed                             │
│    - Start server in background                         │
│    - Wait 3s for startup                                │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│ 6. Compare GetLeavesByRange Responses                   │
│    - Iterate through all entries in batches             │
│    - Call GetLeavesByRange on both servers              │
│    - Compare each field of each leaf                    │
│    - Report all mismatches                              │
└─────────────────────────────────────────────────────────┘
```

## Output

The tool provides detailed logging throughout the test:

```
📡 Connecting to Trillian at localhost:8090...
📝 Creating new Trillian log...
✅ Created log with ID: 123456789
📥 Adding 100 entries to the log...
✅ Added 100 entries
⏳ Waiting for entries to be integrated...
✅ All entries integrated. Tree size: 100, Root hash: abc123...
📦 Generated bucket name: amber-test-550e8400-e29b-41d4-a716-446655440000
🔄 Running amber-polymerize to migrate log 123456789 to bucket amber-test-...
✅ Migration completed
🚀 Starting amber-server on localhost:8093...
📡 Connecting to amber-server at localhost:8093...
🔍 Comparing GetLeavesByRange responses (batch size: 10)...
  Comparing entries [0:10)...
  Comparing entries [10:20)...
  ...
✅ All leaf comparisons passed
✅ All tests passed!
```

## Error Handling

If errors are found, the tool reports them with detailed context:

```
❌ Errors found during comparison:
  - Leaf value mismatch at index 42: Trillian=656e7472792d34322d..., Amber=656e7472792d34332d...
  - Merkle leaf hash mismatch at index 43: Trillian=abc123..., Amber=def456..., Expected=abc123...
```

## Cleanup

The tool automatically:
- Stops amber-server when test completes
- Removes temporary config files

You may want to manually clean up:
- The GCS bucket created for testing
- The Trillian log (if desired)

## Building Separately

You can also build the test tool as a standalone binary:

```bash
go build -o amber-test ./cmd/amber-test
./amber-test test --project-id=your-project --num-entries=100
```

## Troubleshooting

### "Failed to connect to Trillian"
- Ensure `docker compose up` is running
- Check that port 8090 is accessible
- Verify trillian-log-server is healthy: `docker compose ps`

### "amber-polymerize failed"
- Check GCS bucket permissions
- Verify `GOOGLE_CLOUD_PROJECT` environment variable
- Ensure bucket doesn't already exist with conflicting data

### "Failed to start amber-server"
- Check that port 8093 is not in use
- Verify GCS bucket was created successfully
- Check amber-server logs for details

### "Comparison failed"
- This indicates a real bug in the migration process
- Review the specific mismatches reported
- Check if timestamps are being preserved correctly
- Verify hash computation matches between implementations
