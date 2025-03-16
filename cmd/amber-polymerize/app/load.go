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
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	spanner_database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	spanner_instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"cloud.google.com/go/storage"
	"github.com/google/trillian"
	"github.com/google/trillian/types"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	f_log "github.com/transparency-dev/formats/log"
	tessera "github.com/transparency-dev/trillian-tessera"
	"github.com/transparency-dev/trillian-tessera/api/layout"
	tessera_gcp "github.com/transparency-dev/trillian-tessera/storage/gcp"
	"golang.org/x/mod/sumdb/note"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sigs.k8s.io/release-utils/version"
)

type timestamps struct {
	queued     int32
	integrated int32
}

var tsMap map[int64]timestamps = make(map[int64]timestamps)

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

		creds, err := google.FindDefaultCredentials(context.Background())
		if err != nil {
			log.Fatalf("Failed to get default credentials: %v", err)
		}

		// initialize GCS bucket (creating if necessary)
		gcsClient, err := storage.NewClient(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		bucket := gcsClient.Bucket(viper.GetString("bucket_name"))
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
			Bucket:  viper.GetString("bucket_name"),
			Spanner: spanner,
		})
		skey, _, err := note.GenerateKey(rand.Reader, "ignored")
		if err != nil {
			log.Fatal(err)
		}
		signer, err := note.NewSigner(skey)
		if err != nil {
			log.Fatal(err)
		}
		logStorage, _, err := tessera.NewAppender(context.Background(), driver, tessera.WithBatching(1, 2*time.Second), tessera.WithCheckpointSigner(signer))
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
			finish = int64(logRoot.TreeSize) - 1
		}
		if start > finish {
			log.Fatalf("start (%d) cannot be greater than finish (%d)", start, finish)
		}

		// batch reads as to minimize some network overhead
		const batchSize int64 = 32

		var actuallyFetched int64 = 0
		for i := start; i <= finish; i = i + actuallyFetched {
			numToFetch := batchSize
			if i+numToFetch > finish {
				numToFetch = finish - i + 1 // account for 0 offset
			}
			resp, err := tlc.GetLeavesByRange(context.Background(), &trillian.GetLeavesByRangeRequest{
				LogId:      viper.GetInt64("trillian_log_server.log_id"),
				StartIndex: i,
				Count:      numToFetch,
			})
			if err != nil {
				log.Fatal(err)
			}
			actuallyFetched = int64(len(resp.Leaves))
			for _, leaf := range resp.Leaves {
				tsMap[leaf.LeafIndex] = timestamps{
					queued:     leaf.QueueTimestamp.GetNanos(),
					integrated: leaf.IntegrateTimestamp.GetNanos(),
				}
				addFn := logStorage.Add(context.Background(), tessera.NewEntry(leaf.LeafValue))
				index, err := addFn()
				if err != nil {
					log.Fatal(err)
				}
				if (start + int64(index)) != leaf.LeafIndex {
					log.Fatalf("read from index %d, inserted at index %d", leaf.LeafIndex, index)
				}
				log.Printf("wrote %d to log\n", index)
				time.Sleep(1800 * time.Millisecond)
			}
			//TODO: figure out how to write bundle metadata with timestamps
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

		time.Sleep(10 * time.Second)
	},
}

func init() {
	rootCmd.AddCommand(loadCmd)
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
	defer instanceAdmin.Close()

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
	defer adminClient.Close()

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
