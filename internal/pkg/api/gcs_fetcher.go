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

package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/transparency-dev/tessera/api/layout"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

func NewGCSFetcher(bucketName string) (*GCSFetcher, error) {
	// XML API is specifically requested to minimize roundtrips between app and GCS;
	// with XML API, metadata and object are fetched at the same time
	c, err := storage.NewClient(context.Background(), storage.WithXMLReads())
	if err != nil {
		return nil, err
	}

	b := c.Bucket(bucketName)
	if _, err := b.Attrs(context.Background()); err != nil {
		if e, ok := err.(*googleapi.Error); ok && e.Code == 404 {
			return nil, errors.New("bucket not found")
		}
		return nil, fmt.Errorf("error getting bucket attributes: %w", err)
	}

	return &GCSFetcher{
		client: c,
		bucket: b,
	}, nil
}

type GCSFetcher struct {
	client           *storage.Client
	bucket           *storage.BucketHandle
	cachedCheckpoint []byte
	hashPrefixes     map[string]struct{}
	mu               sync.RWMutex
}

func (g *GCSFetcher) FetchWithMetadata(ctx context.Context, path string) ([]byte, map[string]string, error) {
	reader, err := g.bucket.Object(path).NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, nil, fmt.Errorf("object not found at path /%s: %w", path, os.ErrNotExist)
		}
		return nil, nil, err
	}

	defer func() {
		_ = reader.Close() // Best effort close, data already read
	}()
	bytes, err := io.ReadAll(reader)
	if err != nil {
		return nil, nil, err
	}

	return bytes, reader.Metadata(), nil
}

// since this should be constant, we read once from GCS and cache in memory for the life of the process
func (g *GCSFetcher) ReadCheckpoint(ctx context.Context) ([]byte, error) {
	g.mu.RLock()
	if g.cachedCheckpoint != nil {
		defer g.mu.RUnlock()
		return g.cachedCheckpoint, nil
	}
	g.mu.RUnlock()
	cp, _, err := g.FetchWithMetadata(ctx, layout.CheckpointPath)
	if err == nil {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.cachedCheckpoint = cp
	}
	return cp, err
}

func (g *GCSFetcher) ensureHashPrefixes(ctx context.Context) error {
	g.mu.RLock()
	if g.hashPrefixes != nil {
		g.mu.RUnlock()
		return nil
	}
	g.mu.RUnlock()

	query := &storage.Query{
		Prefix: "hashmap/",
	}

	prefixes := make(map[string]struct{})
	it := g.bucket.Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("list hashmap objects: %w", err)
		}
		name := strings.TrimPrefix(attrs.Name, "hashmap/")
		if name == "" {
			continue
		}
		dot := strings.Index(name, ".")
		if dot < 0 {
			continue
		}
		prefix := name[:dot]
		if prefix == "" {
			continue
		}
		prefixes[prefix] = struct{}{}
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	g.hashPrefixes = prefixes
	return nil
}

func (g *GCSFetcher) HashPrefixExists(ctx context.Context, prefix string) (bool, error) {
	if err := g.ensureHashPrefixes(ctx); err != nil {
		return false, err
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, ok := g.hashPrefixes[prefix]
	return ok, nil
}

func (g *GCSFetcher) HashShardPaths(prefix string) (bloomPath, dbPath string) {
	base := fmt.Sprintf("hashmap/%s", prefix)
	return base + ".bloom", base + ".db"
}

func (g *GCSFetcher) ReadTile(ctx context.Context, l, i uint64, p uint8) ([]byte, error) {
	bytes, _, err := g.FetchWithMetadata(ctx, layout.TilePath(l, i, p))
	return bytes, err
}

func (g *GCSFetcher) ReadTileWithMetadata(ctx context.Context, l, i uint64, p uint8) ([]byte, map[string]string, error) {
	return g.FetchWithMetadata(ctx, layout.TilePath(l, i, p))
}

func (g *GCSFetcher) ReadEntryBundle(ctx context.Context, i uint64, p uint8) ([]byte, error) {
	bytes, _, err := g.FetchWithMetadata(ctx, layout.EntriesPath(i, p))
	return bytes, err
}

func (g *GCSFetcher) ReadEntryBundleWithMetadata(ctx context.Context, i uint64, p uint8) ([]byte, map[string]string, error) {
	return g.FetchWithMetadata(ctx, layout.EntriesPath(i, p))
}

func (g *GCSFetcher) ReadObject(ctx context.Context, path string) ([]byte, error) {
	bytes, _, err := g.FetchWithMetadata(ctx, path)
	return bytes, err
}
