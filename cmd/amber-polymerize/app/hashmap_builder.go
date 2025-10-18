package app

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/bits-and-blooms/bloom"
	"github.com/bobcallaway/amber/internal/pkg/hashmap"
	bolt "go.etcd.io/bbolt"
)

// hashIndexBuilder manages sharded hash-to-index mappings backed by BoltDB files
// and companion Bloom filters. Each shard stores mappings for digests that share
// the same hexadecimal prefix of length prefixLength.
type hashIndexBuilder struct {
	prefixLength      uint
	shardDir          string
	falsePositiveRate float64
	expectedPerShard  uint

	shards   map[string]*hashShard
	shardsMu sync.Mutex
}

// hashShard represents the on-disk state for a single prefix shard.
type hashShard struct {
	prefix    string
	db        *bolt.DB
	dbPath    string
	bloom     *bloom.BloomFilter
	bloomPath string

	mu sync.Mutex
}

// newHashIndexBuilder constructs a hashIndexBuilder.
func newHashIndexBuilder(shardDir string, treeSize uint64, prefixLength uint, falsePositiveRate float64) (*hashIndexBuilder, error) {
	if prefixLength == 0 || prefixLength > 64 {
		return nil, fmt.Errorf("prefixLength must be in [1, 64], got %d", prefixLength)
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		return nil, fmt.Errorf("falsePositiveRate must be between 0 and 1 (exclusive), got %f", falsePositiveRate)
	}
	if shardDir == "" {
		return nil, errors.New("shardDir must be provided")
	}
	if err := os.MkdirAll(shardDir, 0o755); err != nil {
		return nil, fmt.Errorf("create shard directory: %w", err)
	}

	shardCount := uint64(1)
	for i := uint(0); i < prefixLength; i++ {
		shardCount *= 16
	}
	if shardCount == 0 {
		return nil, fmt.Errorf("invalid shardCount for prefixLength %d", prefixLength)
	}

	expected := uint(1)
	if treeSize > 0 {
		perShard := math.Ceil(float64(treeSize) / float64(shardCount))
		if perShard >= 1 {
			expected = uint(perShard)
		}
	}

	builder := &hashIndexBuilder{
		prefixLength:      prefixLength,
		shardDir:          shardDir,
		falsePositiveRate: falsePositiveRate,
		expectedPerShard:  expected,
		shards:            make(map[string]*hashShard),
	}

	return builder, nil
}

// Record stores a mapping from the given digest to the provided log index.
func (h *hashIndexBuilder) Record(digest []byte, logIndex int64) error {
	if len(digest) == 0 {
		return errors.New("digest must not be empty")
	}
	if logIndex < 0 {
		return fmt.Errorf("logIndex must be non-negative, got %d", logIndex)
	}

	digestHex := hex.EncodeToString(digest)
	if len(digestHex) < int(h.prefixLength) {
		return fmt.Errorf("digest too short for prefix length %d", h.prefixLength)
	}

	prefix := digestHex[:int(h.prefixLength)]
	suffix := digestHex[int(h.prefixLength):]

	shard, err := h.getOrCreateShard(prefix)
	if err != nil {
		return err
	}

	return shard.add([]byte(suffix), logIndex)
}

// Finalize persists Bloom filters, closes databases, uploads artifacts to GCS,
// and optionally cleans up local files when cleanupFunc is provided.
func (h *hashIndexBuilder) Finalize(ctx context.Context, bucket *storage.BucketHandle, cleanupFunc func(string) error) error {
	if h == nil {
		return nil
	}

	h.shardsMu.Lock()
	shards := make([]*hashShard, 0, len(h.shards))
	for _, shard := range h.shards {
		shards = append(shards, shard)
	}
	h.shardsMu.Unlock()

	for _, shard := range shards {
		if err := shard.flush(); err != nil {
			return fmt.Errorf("flush shard %s: %w", shard.prefix, err)
		}
		if err := shard.upload(ctx, bucket); err != nil {
			return fmt.Errorf("upload shard %s: %w", shard.prefix, err)
		}
		if cleanupFunc != nil {
			if err := cleanupFunc(shard.dbPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("cleanup db %s: %w", shard.dbPath, err)
			}
			if err := cleanupFunc(shard.bloomPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("cleanup bloom %s: %w", shard.bloomPath, err)
			}
		}
	}

	return nil
}

func (h *hashIndexBuilder) getOrCreateShard(prefix string) (*hashShard, error) {
	h.shardsMu.Lock()
	shard, ok := h.shards[prefix]
	h.shardsMu.Unlock()
	if ok {
		return shard, nil
	}

	h.shardsMu.Lock()
	defer h.shardsMu.Unlock()

	if shard, ok := h.shards[prefix]; ok {
		return shard, nil
	}

	newShard, err := h.newShard(prefix)
	if err != nil {
		return nil, err
	}

	h.shards[prefix] = newShard
	return newShard, nil
}

func (h *hashIndexBuilder) newShard(prefix string) (*hashShard, error) {
	dbPath := filepath.Join(h.shardDir, fmt.Sprintf("%s.db", prefix))
	bloomPath := filepath.Join(h.shardDir, fmt.Sprintf("%s.bloom", prefix))

	db, err := bolt.Open(dbPath, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("open shard db: %w", err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(hashmap.HashIndexBucketKey)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create bucket: %w", err)
	}

	expected := h.expectedPerShard
	if expected < 1 {
		expected = 1
	}

	bloomFilter := bloom.NewWithEstimates(expected, h.falsePositiveRate)

	return &hashShard{
		prefix:    prefix,
		db:        db,
		dbPath:    dbPath,
		bloom:     bloomFilter,
		bloomPath: bloomPath,
	}, nil
}

func (s *hashShard) add(key []byte, logIndex int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(logIndex))

	if err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(hashmap.HashIndexBucketKey)
		if b == nil {
			return errors.New("hash index bucket missing")
		}
		return b.Put(key, buf[:])
	}); err != nil {
		return fmt.Errorf("put leaf index: %w", err)
	}

	s.bloom.Add(key)

	return nil
}

func (s *hashShard) flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db != nil {
		if err := s.db.Sync(); err != nil {
			_ = s.db.Close()
			return fmt.Errorf("sync db: %w", err)
		}
		if err := s.db.Close(); err != nil {
			return fmt.Errorf("close db: %w", err)
		}
		s.db = nil
	}

	f, err := os.Create(s.bloomPath)
	if err != nil {
		return fmt.Errorf("create bloom file: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()

	if _, err := s.bloom.WriteTo(f); err != nil {
		return fmt.Errorf("write bloom filter: %w", err)
	}

	return nil
}

func (s *hashShard) upload(ctx context.Context, bucket *storage.BucketHandle) error {
	if bucket == nil {
		return errors.New("bucket handle is nil")
	}

	dbObject := bucket.Object(filepath.ToSlash(filepath.Join("hashmap", fmt.Sprintf("%s.db", s.prefix))))
	if err := uploadFile(ctx, dbObject, s.dbPath); err != nil {
		return fmt.Errorf("upload db: %w", err)
	}

	bloomObject := bucket.Object(filepath.ToSlash(filepath.Join("hashmap", fmt.Sprintf("%s.bloom", s.prefix))))
	if err := uploadFile(ctx, bloomObject, s.bloomPath); err != nil {
		return fmt.Errorf("upload bloom: %w", err)
	}

	return nil
}

func uploadFile(ctx context.Context, obj *storage.ObjectHandle, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open file %s: %w", path, err)
	}
	defer func() {
		_ = file.Close()
	}()

	writer := obj.NewWriter(ctx)
	if _, err := io.Copy(writer, file); err != nil {
		_ = writer.Close()
		return fmt.Errorf("write object: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}

	return nil
}
