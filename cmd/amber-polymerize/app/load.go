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
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strings"

	spanner_database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	spanner_instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/bobcallaway/amber/internal/pkg/gcs"
	"github.com/bobcallaway/amber/internal/pkg/hashmap"
	tsmd "github.com/bobcallaway/amber/internal/pkg/metadata"
	"github.com/google/trillian"
	"github.com/google/trillian/types"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	f_log "github.com/transparency-dev/formats/log"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/client"
	tessera_gcp "github.com/transparency-dev/tessera/storage/gcp"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sigs.k8s.io/release-utils/version"
)

// bundleTimestamps stores timestamp pairs for all entries in a bundle
type bundleTimestamps []tsmd.TimestampPair

// timestampStore maps bundle index -> timestamps for that bundle
var timestampStore = make(map[uint64]bundleTimestamps)

// loadCmd represents the load command
var loadCmd = &cobra.Command{
	Use:   "load",
	Short: "loads all entries from a frozen Trillian log and writes contents into C2SP tlog-tiles format inside GCS bucket",
	Long:  "loads all entries from a frozen Trillian log and writes contents into C2SP tlog-tiles format inside GCS bucket",
	Run: func(_ *cobra.Command, _ []string) {
		// from https://github.com/golang/glog/commit/fca8c8854093a154ff1eb580aae10276ad6b1b5f
		_ = flag.CommandLine.Parse([]string{})

		vi := version.GetVersionInfo()
		viStr, err := vi.JSONString()
		if err != nil {
			viStr = vi.String()
		}
		log.Printf("starting amber-polymerize @ %v", viStr)

		// get trillian client
		cc, err := grpc.NewClient(fmt.Sprintf("dns:%s:%s", viper.GetString("trillian_log_server.address"), viper.GetString("trillian_log_server.port")), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatal(err)
		}
		tlc := trillian.NewTrillianLogClient(cc)

		// attempt connection to gRPC endpoint by getting STH
		resp, err := tlc.GetLatestSignedLogRoot(context.Background(), &trillian.GetLatestSignedLogRootRequest{
			LogId: int64(viper.GetInt("trillian_log_server.log_id")),
		})
		if err != nil {
			log.Fatal(err)
		}
		logRoot := &types.LogRootV1{}
		if err := logRoot.UnmarshalBinary(resp.SignedLogRoot.LogRoot); err != nil {
			log.Fatal(err)
		}
		cp := f_log.Checkpoint{
			Origin: viper.GetString("origin"),
			Hash:   logRoot.RootHash,
			Size:   logRoot.TreeSize,
		}
		cpBytes := cp.Marshal()

		if viper.GetUint64("finish") > logRoot.TreeSize {
			log.Fatalf("asked for entries outside of known range (treeSize = %d, finish = %d)", logRoot.TreeSize, viper.GetUint64("finish"))
		}

		// Create metadata provider that looks up timestamps from timestampStore
		metadataProvider := func(objectPath string) map[string]string {
			// Only add metadata for entry bundle objects
			// Entry paths look like: tile/entries/000 or tile/entries/x000/111
			if !strings.HasPrefix(objectPath, "tile/entries/") {
				return nil
			}

			// Parse the entry bundle index from the path using ParseTileIndexPartial
			// Extract just the index portion after "tile/entries/"
			indexPart := strings.TrimPrefix(objectPath, "tile/entries/")
			bundleIndex, partial, err := layout.ParseTileIndexPartial(indexPart)
			if err != nil {
				log.Printf("Warning: failed to parse entry path %s: %v", objectPath, err)
				return nil
			}

			// Look up timestamps for this bundle
			timestamps, ok := timestampStore[bundleIndex]
			if !ok || len(timestamps) == 0 {
				log.Printf("Warning: no timestamps found for bundle %d (path: %s)", bundleIndex, objectPath)
				return nil
			}

			// Verify the number of timestamps matches the expected bundle size
			expectedCount := int(layout.EntryBundleWidth)
			if partial > 0 {
				expectedCount = int(partial)
			}
			if len(timestamps) != expectedCount {
				log.Fatalf("Warning: timestamp count mismatch for bundle %d: got %d, expected %d (path: %s)",
					bundleIndex, len(timestamps), expectedCount, objectPath)
			}

			return tsmd.BuildTimestampMetadata(timestamps)
		}

		// Initialize GCS client with dynamic metadata injection
		gcsClientWithMetadata, err := gcs.NewClientWithMetadata(context.Background(), metadataProvider)
		if err != nil {
			log.Fatal(err)
		}

		creds, err := google.FindDefaultCredentials(context.Background())
		if err != nil {
			log.Fatalf("Failed to get default credentials: %v", err)
		}

		bucket := gcsClientWithMetadata.Bucket(viper.GetString("bucket_name"))
		if _, err := bucket.Attrs(context.Background()); err != nil {
			if e, ok := err.(*googleapi.Error); ok && e.Code == 404 {
				// bucket doesn't exist, create it
				if err := bucket.Create(context.Background(), creds.ProjectID, nil); err != nil {
					log.Fatal(err)
				}
			} else {
				log.Fatal(err)
			}
		}

		// set up tessera GCP client (using spanner emulator)
		spanner, err := createSpannerInstanceAndDB()
		if err != nil {
			log.Fatal(err)
		}
		driver, err := tessera_gcp.New(context.Background(), tessera_gcp.Config{
			Bucket:    viper.GetString("bucket_name"),
			GCSClient: gcsClientWithMetadata,
			Spanner:   spanner,
		})
		if err != nil {
			log.Fatal(err)
		}
		m, err := tessera.NewMigrationTarget(context.Background(), driver, tessera.NewMigrationOptions())
		if err != nil {
			log.Fatal(err)
		}

		// begin scrape of range
		start := viper.GetInt64("start")
		if start < 0 {
			log.Fatalf("start (%d) must be greater than 0", start)
		}
		finish := viper.GetInt64("finish")
		if finish == -1 {
			finish = int64(logRoot.TreeSize)
		}
		if start > finish {
			log.Fatalf("start (%d) cannot be greater than finish (%d)", start, finish)
		}

		falsePositiveRate := viper.GetFloat64("hashmap.false_positive_rate")
		if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
			log.Fatalf("hashmap.false_positive_rate must be between 0 and 1, got %f", falsePositiveRate)
		}

		hashMapDir := viper.GetString("hashmap.temp_dir")
		cleanupTempDir := false
		if hashMapDir == "" {
			hashMapDir, err = os.MkdirTemp("", "amber-hashmap-")
			if err != nil {
				log.Fatalf("create temporary hash map directory: %v", err)
			}
			cleanupTempDir = true
		} else {
			if err := os.MkdirAll(hashMapDir, 0o755); err != nil {
				log.Fatalf("ensure hash map directory %s exists: %v", hashMapDir, err)
			}
		}

		prefixLength := hashmap.CalculateShardPrefixLength(logRoot.TreeSize)
		indexBuilder, err := newHashIndexBuilder(hashMapDir, logRoot.TreeSize, prefixLength, falsePositiveRate)
		if err != nil {
			log.Fatalf("init hash index builder: %v", err)
		}
		log.Printf("hash index builder configured with prefix length %d and %.4f false-positive rate", prefixLength, falsePositiveRate)

		getBundle := bundleFetcher(tlc, indexBuilder)
		numWorkers := uint(10)
		if err := m.Migrate(context.Background(), numWorkers, uint64(finish), logRoot.RootHash, getBundle); err != nil {
			log.Fatalf("Migrate: %v", err)
		}

		var cleanupFn func(string) error
		if cleanupTempDir {
			cleanupFn = os.Remove
		}
		if err := indexBuilder.Finalize(context.Background(), bucket, cleanupFn); err != nil {
			log.Fatalf("finalize hash index: %v", err)
		}
		if cleanupTempDir {
			if err := os.RemoveAll(hashMapDir); err != nil {
				log.Printf("Warning: failed to clean up hash map directory %s: %v", hashMapDir, err)
			}
		}

		// Write final checkpoint with frozen timestamp
		cpHandle := bucket.Object(layout.CheckpointPath).NewWriter(context.Background())
		// these two settings are manually copied from tessera
		cpHandle.CacheControl = "no-cache"
		cpHandle.ContentType = "text/plain; charset=utf-8"
		// this is set so we can persist the "frozen" timestamp
		cpHandle.Metadata = map[string]string{
			"TimestampNanos": fmt.Sprintf("%d", logRoot.TimestampNanos),
		}
		if _, err := cpHandle.Write(cpBytes); err != nil {
			log.Fatal(err)
		}
		if err := cpHandle.Close(); err != nil {
			log.Fatal(err)
		}
	},
}

func bundleFetcher(tlc trillian.TrillianLogClient, indexBuilder *hashIndexBuilder) client.EntryBundleFetcherFunc {
	batchSize := uint64(32)

	return func(ctx context.Context, idx uint64, p uint8) ([]byte, error) {
		remain := uint64(p)
		if remain == 0 {
			remain = layout.EntryBundleWidth
		}
		ret := make([]byte, 0, 64<<10)
		startIdx := idx * layout.EntryBundleWidth
		fetched := uint64(0)

		// Collect timestamps for this bundle
		bundleTimestamps := make([]tsmd.TimestampPair, 0, remain)

		for fetched < remain {
			numToFetch := min(remain, batchSize)
			leafIdx := startIdx + uint64(fetched)
			resp, err := tlc.GetLeavesByRange(ctx, &trillian.GetLeavesByRangeRequest{
				LogId:      viper.GetInt64("trillian_log_server.log_id"),
				StartIndex: int64(leafIdx),
				Count:      int64(numToFetch),
			})
			if err != nil {
				return nil, fmt.Errorf("trillian GetLeavesByRange(%d, %d): %v", leafIdx, numToFetch, err)
			}
			for _, l := range resp.Leaves {
				currentIndexUint := startIdx + fetched
				if currentIndexUint > math.MaxInt64 {
					return nil, fmt.Errorf("log index %d exceeds int64 limits", currentIndexUint)
				}
				currentIndex := int64(currentIndexUint)

				// Store timestamp for this entry
				// Convert timestamppb.Timestamp to nanoseconds since epoch
				bundleTimestamps = append(bundleTimestamps, tsmd.TimestampPair{
					QueueTimestampNanos:     l.QueueTimestamp.AsTime().UnixNano(),
					IntegrateTimestampNanos: l.IntegrateTimestamp.AsTime().UnixNano(),
				})

				if indexBuilder != nil {
					if len(l.MerkleLeafHash) == 0 {
						return nil, fmt.Errorf("missing MerkleLeafHash for leaf %d", currentIndex)
					}
					if err := indexBuilder.Record(l.MerkleLeafHash, currentIndex); err != nil {
						return nil, fmt.Errorf("record hash index for leaf %d: %w", currentIndex, err)
					}
				}

				bd := tessera.NewEntry(l.LeafValue).MarshalBundleData(uint64(currentIndex))
				ret = append(ret, bd...)
				fetched++
			}
		}

		// Store timestamps for this bundle indexed by bundle index
		timestampStore[idx] = bundleTimestamps

		return ret, nil
	}
}

func init() {
	rootCmd.AddCommand(loadCmd)

	loadCmd.Flags().Float64("hashmap_false_positive_rate", 0.01, "Target false-positive rate for hash prefix Bloom filters")
	if err := viper.BindPFlag("hashmap.false_positive_rate", loadCmd.Flags().Lookup("hashmap_false_positive_rate")); err != nil {
		log.Fatal(err)
	}

	loadCmd.Flags().String("hashmap_temp_dir", "", "Directory for temporary hash map shards (default: generated in system temp)")
	if err := viper.BindPFlag("hashmap.temp_dir", loadCmd.Flags().Lookup("hashmap_temp_dir")); err != nil {
		log.Fatal(err)
	}
}

func createSpannerInstanceAndDB() (string, error) {
	emulatorHost := os.Getenv("SPANNER_EMULATOR_HOST")
	if emulatorHost == "" {
		return "", fmt.Errorf("SPANNER_EMULATOR_HOST not set")
	}

	instanceAdmin, err := spanner_instance.NewInstanceAdminClient(context.Background(), option.WithEndpoint(emulatorHost), option.WithoutAuthentication())
	if err != nil {
		return "", fmt.Errorf("create instance client %w", err)
	}
	defer func() {
		if err := instanceAdmin.Close(); err != nil {
			log.Printf("Warning: failed to close instance admin client: %v", err)
		}
	}()

	uuid := uuid.NewString()
	name := fmt.Sprintf("amber-%s", uuid)
	op, err := instanceAdmin.CreateInstance(context.Background(), &instancepb.CreateInstanceRequest{
		Parent:     "projects/amber-polymerize",
		InstanceId: name,
		Instance: &instancepb.Instance{
			Name:            fmt.Sprintf("projects/amber-polymerize/instances/%s", name),
			Config:          "projects/amber-polymerize/instanceConfigs/regional-us-central1",
			DisplayName:     "amber",
			ProcessingUnits: 100,
			Edition:         instancepb.Instance_STANDARD,
		},
	})
	if err != nil {
		return "", fmt.Errorf("create instance %w", err)
	}
	instance, err := op.Wait(context.Background())
	if err != nil {
		return "", fmt.Errorf("create instance op wait %w", err)
	}

	adminClient, err := spanner_database.NewDatabaseAdminClient(context.Background())
	if err != nil {
		return "", fmt.Errorf("create database client %w", err)
	}
	defer func() {
		if err := adminClient.Close(); err != nil {
			log.Printf("Warning: failed to close database admin client: %v", err)
		}
	}()

	op2, err := adminClient.CreateDatabase(context.Background(), &databasepb.CreateDatabaseRequest{
		Parent:          instance.Name,
		CreateStatement: "CREATE DATABASE amber",
	})
	if err != nil {
		return "", fmt.Errorf("create database %w", err)
	}

	db, err := op2.Wait(context.Background())
	if err != nil {
		return "", fmt.Errorf("create database op wait %w", err)
	}

	return db.Name, nil
}
