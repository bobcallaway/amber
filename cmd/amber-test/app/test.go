//
// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package app

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/google/trillian"
	"github.com/google/trillian/types"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
)

var (
	trillianAddress string
	trillianPort    string
	numEntries      int
	bucketName      string
	projectID       string
	origin          string
	amberServerAddr string
	amberServerPort string
	batchSize       int
)

var testCmd = &cobra.Command{
	Use:   "test",
	Short: "Run end-to-end test of amber-polymerize migration",
	Long: `Creates a new Trillian log, populates it with entries, runs amber-polymerize 
to migrate to tessera tile backend, and validates the migration by comparing 
GetLeavesByRange responses between Trillian and amber-server.`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := runTest(); err != nil {
			log.Fatalf("Test failed: %v", err)
		}
		log.Println("✅ All tests passed!")
	},
}

func init() {
	testCmd.Flags().StringVar(&trillianAddress, "trillian-address", "localhost", "Trillian log server address")
	testCmd.Flags().StringVar(&trillianPort, "trillian-port", "8090", "Trillian log server port")
	testCmd.Flags().IntVar(&numEntries, "num-entries", 100, "Number of entries to add to the log")
	testCmd.Flags().StringVar(&bucketName, "bucket", "", "GCS bucket name (will be auto-generated if not provided)")
	testCmd.Flags().StringVar(&projectID, "project-id", "", "GCP project ID (required)")
	testCmd.Flags().StringVar(&origin, "origin", "amber-test-log", "Log origin/name")
	testCmd.Flags().StringVar(&amberServerAddr, "amber-server-address", "localhost", "Amber server address")
	testCmd.Flags().StringVar(&amberServerPort, "amber-server-port", "8093", "Amber server port")
	testCmd.Flags().IntVar(&batchSize, "batch-size", 10, "Batch size for comparing entries")

	_ = testCmd.MarkFlagRequired("project-id")
}

func runTest() error {
	ctx := context.Background()

	// Step 1: Connect to Trillian
	log.Printf("📡 Connecting to Trillian at %s:%s...", trillianAddress, trillianPort)
	trillianConn, err := grpc.NewClient(
		fmt.Sprintf("dns:%s:%s", trillianAddress, trillianPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to Trillian: %w", err)
	}
	defer func() {
		_ = trillianConn.Close()
	}()

	trillianClient := trillian.NewTrillianLogClient(trillianConn)
	trillianAdmin := trillian.NewTrillianAdminClient(trillianConn)

	// Step 2: Create a new log
	log.Println("📝 Creating new Trillian log...")
	logID, err := createLog(ctx, trillianAdmin)
	if err != nil {
		return fmt.Errorf("failed to create log: %w", err)
	}
	log.Printf("✅ Created log with ID: %d", logID)

	// Initialize the log
	log.Println("🔧 Initializing Trillian log...")
	_, err = trillianClient.InitLog(ctx, &trillian.InitLogRequest{
		LogId: logID,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize log: %w", err)
	}
	log.Println("✅ Log initialized")

	// Step 3: Add entries to the log
	log.Printf("📥 Adding %d entries to the log...", numEntries)
	entries, err := addEntries(ctx, trillianClient, logID, numEntries)
	if err != nil {
		return fmt.Errorf("failed to add entries: %w", err)
	}
	log.Printf("✅ Added %d entries", len(entries))

	// Wait for entries to be integrated
	log.Println("⏳ Waiting for entries to be integrated...")
	treeSize, rootHash, frozenTime, err := waitForIntegration(ctx, trillianClient, logID, int64(numEntries))
	if err != nil {
		return fmt.Errorf("failed to wait for integration: %w", err)
	}
	log.Printf("✅ All entries integrated. Tree size: %d, Root hash: %s", treeSize, hex.EncodeToString(rootHash))

	// Step 4: Generate bucket name if not provided
	if bucketName == "" {
		bucketName = fmt.Sprintf("amber-test-%s", uuid.New().String())
		log.Printf("📦 Generated bucket name: %s", bucketName)
	}

	// Step 5: Run amber-polymerize
	log.Printf("🔄 Running amber-polymerize to migrate log %d to bucket %s...", logID, bucketName)
	log.Printf("   Trillian: %s:%s", trillianAddress, trillianPort)
	log.Printf("   Storage Emulator: %s", os.Getenv("STORAGE_EMULATOR_HOST"))
	log.Printf("   Spanner Emulator: %s", os.Getenv("SPANNER_EMULATOR_HOST"))
	if err := runPolymerize(logID, frozenTime, bucketName); err != nil {
		return fmt.Errorf("failed to run amber-polymerize: %w", err)
	}
	log.Println("✅ Migration completed")

	// Step 6: Start amber-server
	log.Printf("🚀 Starting amber-server on %s:%s...", amberServerAddr, amberServerPort)
	amberServerCmd, err := startAmberServer(logID, frozenTime, bucketName)
	if err != nil {
		return fmt.Errorf("failed to start amber-server: %w", err)
	}
	defer func() {
		log.Println("🛑 Stopping amber-server...")
		_ = amberServerCmd.Process.Kill()
	}()

	// Wait for amber-server to start
	time.Sleep(3 * time.Second)

	// Step 7: Connect to amber-server
	log.Printf("📡 Connecting to amber-server at %s:%s...", amberServerAddr, amberServerPort)
	amberConn, err := grpc.NewClient(
		fmt.Sprintf("dns:%s:%s", amberServerAddr, amberServerPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to amber-server: %w", err)
	}
	defer func() {
		_ = amberConn.Close()
	}()

	amberClient := trillian.NewTrillianLogClient(amberConn)

	// Step 8: Compare GetLeavesByRange responses
	log.Printf("🔍 Comparing GetLeavesByRange responses (batch size: %d)...", batchSize)
	if err := compareLeaves(ctx, trillianClient, amberClient, logID, treeSize); err != nil {
		return fmt.Errorf("comparison failed: %w", err)
	}
	log.Println("✅ All leaf comparisons passed")

	return nil
}

func createLog(ctx context.Context, admin trillian.TrillianAdminClient) (int64, error) {
	tree := &trillian.Tree{
		TreeState:       trillian.TreeState_ACTIVE,
		TreeType:        trillian.TreeType_LOG,
		MaxRootDuration: durationpb.New(time.Hour),
	}

	resp, err := admin.CreateTree(ctx, &trillian.CreateTreeRequest{Tree: tree})
	if err != nil {
		return 0, err
	}

	return resp.TreeId, nil
}

func addEntries(ctx context.Context, client trillian.TrillianLogClient, logID int64, count int) ([][]byte, error) {
	entries := make([][]byte, 0, count)

	for i := 0; i < count; i++ {
		// Generate random entry data
		data := make([]byte, 32)
		if _, err := rand.Read(data); err != nil {
			return nil, fmt.Errorf("failed to generate random data: %w", err)
		}

		// Add timestamp and counter for uniqueness
		entry := []byte(fmt.Sprintf("entry-%d-%d-%s", i, time.Now().UnixNano(), hex.EncodeToString(data)))
		entries = append(entries, entry)

		// Queue each leaf individually
		_, err := client.QueueLeaf(ctx, &trillian.QueueLeafRequest{
			LogId: logID,
			Leaf: &trillian.LogLeaf{
				LeafValue: entry,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("failed to queue leaf %d: %w", i, err)
		}
	}

	return entries, nil
}

func waitForIntegration(ctx context.Context, client trillian.TrillianLogClient, logID int64, expectedSize int64) (uint64, []byte, int64, error) {
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return 0, nil, 0, fmt.Errorf("timeout waiting for integration")
		case <-ticker.C:
			resp, err := client.GetLatestSignedLogRoot(ctx, &trillian.GetLatestSignedLogRootRequest{
				LogId: logID,
			})
			if err != nil {
				return 0, nil, 0, err
			}

			logRoot := &types.LogRootV1{}
			if err := logRoot.UnmarshalBinary(resp.SignedLogRoot.LogRoot); err != nil {
				return 0, nil, 0, err
			}

			if int64(logRoot.TreeSize) >= expectedSize {
				return logRoot.TreeSize, logRoot.RootHash, int64(logRoot.TimestampNanos), nil
			}
		}
	}
}

func runPolymerize(logID int64, frozenTime int64, bucket string) error {
	// Get the path to amber-polymerize binary
	polymerizeBin := "./amber-polymerize"
	if _, err := os.Stat(polymerizeBin); os.IsNotExist(err) {
		// Try to build it
		log.Println("🔨 Building amber-polymerize...")
		buildCmd := exec.Command("go", "build", "-o", polymerizeBin, "./cmd/amber-polymerize")
		buildCmd.Stdout = os.Stdout
		buildCmd.Stderr = os.Stderr
		if err := buildCmd.Run(); err != nil {
			return fmt.Errorf("failed to build amber-polymerize: %w", err)
		}
		log.Println("✅ Built amber-polymerize")
	} else {
		log.Println("♻️  Using existing amber-polymerize binary")
	}

	// Resolve hostname to IP if needed
	address := trillianAddress
	if address == "localhost" {
		address = "127.0.0.1"
	}

	log.Printf("🚀 Launching amber-polymerize with:")
	log.Printf("   --trillian_log_server.address=%s", address)
	log.Printf("   --trillian_log_server.port=%s", trillianPort)
	log.Printf("   --trillian_log_server.log_id=%d", logID)
	log.Printf("   --bucket_name=%s", bucket)
	log.Printf("   --origin=%s", origin)

	// Log environment variables for debugging
	storageHost := os.Getenv("STORAGE_EMULATOR_HOST")
	spannerHost := os.Getenv("SPANNER_EMULATOR_HOST")
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	log.Printf("   STORAGE_EMULATOR_HOST=%s", storageHost)
	log.Printf("   SPANNER_EMULATOR_HOST=%s", spannerHost)
	log.Printf("   GOOGLE_CLOUD_PROJECT=%s", projectID)

	// Run amber-polymerize with command-line flags
	cmd := exec.Command(polymerizeBin, "load",
		"--trillian_log_server.address", address,
		"--trillian_log_server.port", trillianPort,
		"--trillian_log_server.log_id", fmt.Sprintf("%d", logID),
		"--bucket_name", bucket,
		"--origin", origin,
		"--finish", "-1", // Copy all entries
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	// Pass through environment variables including emulator hosts
	cmd.Env = os.Environ()

	log.Println("⏳ Running amber-polymerize...")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("amber-polymerize failed: %w", err)
	}

	return nil
}

func startAmberServer(logID int64, frozenTime int64, bucket string) (*exec.Cmd, error) {
	// Get the path to amber-server binary
	serverBin := "./amber-server"
	if _, err := os.Stat(serverBin); os.IsNotExist(err) {
		// Try to build it
		log.Println("🔨 Building amber-server...")
		buildCmd := exec.Command("go", "build", "-o", serverBin, "./cmd/amber-server")
		buildCmd.Stdout = os.Stdout
		buildCmd.Stderr = os.Stderr
		if err := buildCmd.Run(); err != nil {
			return nil, fmt.Errorf("failed to build amber-server: %w", err)
		}
	}

	// Create a temporary config file
	configPath := filepath.Join(os.TempDir(), fmt.Sprintf("amber-server-config-%d.yaml", time.Now().Unix()))

	configContent := fmt.Sprintf(`address: %s
port: %s

logMap:
  %d:
    frozenTime: %d
    bucketName: %s
`, amberServerAddr, amberServerPort, logID, frozenTime, bucket)

	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		return nil, fmt.Errorf("failed to write server config file: %w", err)
	}

	// Start amber-server
	cmd := exec.Command(serverBin, "serve", "--config", configPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	// Pass through environment variables including emulator hosts
	cmd.Env = os.Environ()

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start amber-server: %w", err)
	}

	return cmd, nil
}

func compareLeaves(ctx context.Context, trillianClient, amberClient trillian.TrillianLogClient, logID int64, treeSize uint64) error {
	var errorsFound []string

	// Iterate through all entries in batches
	for startIdx := int64(0); startIdx < int64(treeSize); startIdx += int64(batchSize) {
		count := int64(batchSize)
		if startIdx+count > int64(treeSize) {
			count = int64(treeSize) - startIdx
		}

		log.Printf("  Comparing entries [%d:%d)...", startIdx, startIdx+count)

		// Get leaves from Trillian
		trillianResp, err := trillianClient.GetLeavesByRange(ctx, &trillian.GetLeavesByRangeRequest{
			LogId:      logID,
			StartIndex: startIdx,
			Count:      count,
		})
		if err != nil {
			return fmt.Errorf("trillian GetLeavesByRange(%d, %d) failed: %w", startIdx, count, err)
		}

		// Get leaves from amber-server
		amberResp, err := amberClient.GetLeavesByRange(ctx, &trillian.GetLeavesByRangeRequest{
			LogId:      logID,
			StartIndex: startIdx,
			Count:      count,
		})
		if err != nil {
			return fmt.Errorf("amber GetLeavesByRange(%d, %d) failed: %w", startIdx, count, err)
		}

		// Compare responses
		if len(trillianResp.Leaves) != len(amberResp.Leaves) {
			errorsFound = append(errorsFound,
				fmt.Sprintf("Leaf count mismatch at range [%d:%d): Trillian=%d, Amber=%d",
					startIdx, startIdx+count, len(trillianResp.Leaves), len(amberResp.Leaves)))
			continue
		}

		for i := range trillianResp.Leaves {
			tLeaf := trillianResp.Leaves[i]
			aLeaf := amberResp.Leaves[i]

			// Compare leaf index
			if tLeaf.LeafIndex != aLeaf.LeafIndex {
				errorsFound = append(errorsFound,
					fmt.Sprintf("Leaf index mismatch at position %d: Trillian=%d, Amber=%d",
						i, tLeaf.LeafIndex, aLeaf.LeafIndex))
			}

			// Compare leaf value
			if string(tLeaf.LeafValue) != string(aLeaf.LeafValue) {
				errorsFound = append(errorsFound,
					fmt.Sprintf("Leaf value mismatch at index %d: Trillian=%s, Amber=%s",
						tLeaf.LeafIndex, hex.EncodeToString(tLeaf.LeafValue[:min(16, len(tLeaf.LeafValue))]),
						hex.EncodeToString(aLeaf.LeafValue[:min(16, len(aLeaf.LeafValue))])))
			}

			// Compare merkle leaf hash
			if string(tLeaf.MerkleLeafHash) != string(aLeaf.MerkleLeafHash) {
				// Recompute hash to verify
				expectedHash := sha256.Sum256(append([]byte{0}, tLeaf.LeafValue...))
				errorsFound = append(errorsFound,
					fmt.Sprintf("Merkle leaf hash mismatch at index %d: Trillian=%s, Amber=%s, Expected=%s",
						tLeaf.LeafIndex,
						hex.EncodeToString(tLeaf.MerkleLeafHash),
						hex.EncodeToString(aLeaf.MerkleLeafHash),
						hex.EncodeToString(expectedHash[:])))
			}

			// Compare timestamps (queue timestamp)
			if tLeaf.QueueTimestamp != nil && aLeaf.QueueTimestamp != nil {
				if tLeaf.QueueTimestamp.AsTime().UnixNano() != aLeaf.QueueTimestamp.AsTime().UnixNano() {
					errorsFound = append(errorsFound,
						fmt.Sprintf("Queue timestamp mismatch at index %d: Trillian=%d, Amber=%d",
							tLeaf.LeafIndex,
							tLeaf.QueueTimestamp.AsTime().UnixNano(),
							aLeaf.QueueTimestamp.AsTime().UnixNano()))
				}
			}

			// Compare integrate timestamp
			if tLeaf.IntegrateTimestamp != nil && aLeaf.IntegrateTimestamp != nil {
				if tLeaf.IntegrateTimestamp.AsTime().UnixNano() != aLeaf.IntegrateTimestamp.AsTime().UnixNano() {
					errorsFound = append(errorsFound,
						fmt.Sprintf("Integrate timestamp mismatch at index %d: Trillian=%d, Amber=%d",
							tLeaf.LeafIndex,
							tLeaf.IntegrateTimestamp.AsTime().UnixNano(),
							aLeaf.IntegrateTimestamp.AsTime().UnixNano()))
				}
			}

			// Compare inclusion proofs by hash for this leaf
			tProofResp, err := trillianClient.GetInclusionProofByHash(ctx, &trillian.GetInclusionProofByHashRequest{
				LogId:    logID,
				LeafHash: tLeaf.MerkleLeafHash,
				TreeSize: int64(treeSize),
			})
			if err != nil {
				errorsFound = append(errorsFound,
					fmt.Sprintf("Trillian GetInclusionProofByHash failed at index %d: %v", tLeaf.LeafIndex, err))
				continue
			}
			aProofResp, err := amberClient.GetInclusionProofByHash(ctx, &trillian.GetInclusionProofByHashRequest{
				LogId:    logID,
				LeafHash: tLeaf.MerkleLeafHash,
				TreeSize: int64(treeSize),
			})
			if err != nil {
				errorsFound = append(errorsFound,
					fmt.Sprintf("Amber GetInclusionProofByHash failed at index %d: %v", tLeaf.LeafIndex, err))
				continue
			}

			if len(tProofResp.Proof) == 0 || len(aProofResp.Proof) == 0 {
				errorsFound = append(errorsFound,
					fmt.Sprintf("Inclusion proof missing at index %d: Trillian proofs=%d Amber proofs=%d",
						tLeaf.LeafIndex, len(tProofResp.Proof), len(aProofResp.Proof)))
				continue
			}
			tProof := tProofResp.Proof[0]
			aProof := aProofResp.Proof[0]

			if tProof.LeafIndex != tLeaf.LeafIndex || aProof.LeafIndex != aLeaf.LeafIndex || tProof.LeafIndex != aProof.LeafIndex {
				errorsFound = append(errorsFound,
					fmt.Sprintf("Inclusion proof leafIndex mismatch at index %d: TrillianProof=%d AmberProof=%d", tLeaf.LeafIndex, tProof.LeafIndex, aProof.LeafIndex))
			}
			if len(tProof.Hashes) != len(aProof.Hashes) {
				errorsFound = append(errorsFound,
					fmt.Sprintf("Inclusion proof hash count mismatch at index %d: Trillian=%d Amber=%d",
						tLeaf.LeafIndex, len(tProof.Hashes), len(aProof.Hashes)))
			} else {
				for j := range tProof.Hashes {
					if !bytes.Equal(tProof.Hashes[j], aProof.Hashes[j]) {
						errorsFound = append(errorsFound,
							fmt.Sprintf("Inclusion proof hash mismatch at index %d step %d: Trillian=%s Amber=%s",
								tLeaf.LeafIndex, j, hex.EncodeToString(tProof.Hashes[j]), hex.EncodeToString(aProof.Hashes[j])))
						break
					}
				}
			}
		}
	}

	if len(errorsFound) > 0 {
		log.Println("❌ Errors found during comparison:")
		for _, err := range errorsFound {
			log.Printf("  - %s", err)
		}
		return fmt.Errorf("found %d errors during comparison", len(errorsFound))
	}

	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
