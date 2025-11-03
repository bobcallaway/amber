package app

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"cloud.google.com/go/storage"
	"github.com/FastFilter/xorfilter"
	"github.com/bobcallaway/amber/internal/pkg/hashmap"
	"github.com/cespare/xxhash/v2"
	mph "github.com/dgryski/go-mph"
)

const (
	leafDigestLengthBytes = sha256.Size
	shardPrefixByteLen    = hashmap.ShardPrefixLengthHex / 2
	shardSuffixByteLen    = leafDigestLengthBytes - shardPrefixByteLen
)

type hashIndexBuilder struct {
	shardDir string
	treeSize uint64

	shards sync.Map // map[string]*hashShard
}

type hashShard struct {
	prefix    string
	path      string
	file      *os.File
	writer    *bufio.Writer
	count     int
	suffixLen int
	treeSize  uint64

	mu sync.Mutex
}

func newHashIndexBuilder(shardDir string, treeSize uint64) (*hashIndexBuilder, error) {
	if shardDir == "" {
		return nil, errors.New("shardDir must be provided")
	}
	if err := os.MkdirAll(shardDir, 0o755); err != nil {
		return nil, fmt.Errorf("create shard directory: %w", err)
	}

	if shardSuffixByteLen <= 0 {
		return nil, fmt.Errorf("invalid suffix length computed: %d", shardSuffixByteLen)
	}

	h := &hashIndexBuilder{
		shardDir: shardDir,
		treeSize: treeSize,
	}

	return h, nil
}

func (h *hashIndexBuilder) Record(digest []byte, logIndex uint64) error {
	if len(digest) != leafDigestLengthBytes {
		return fmt.Errorf("digest length = %d, want %d", len(digest), leafDigestLengthBytes)
	}
	prefix := hex.EncodeToString(digest[:shardPrefixByteLen])

	// Avoid allocation - use slice directly
	suffix := digest[shardPrefixByteLen:]

	shard, err := h.getOrCreateShard(prefix)
	if err != nil {
		return err
	}

	return shard.appendRecord(suffix, logIndex)
}

func (h *hashIndexBuilder) Finalize(ctx context.Context, bucket *storage.BucketHandle, cleanupFunc func(string) error) error {
	if bucket == nil {
		return errors.New("bucket handle is nil")
	}

	// Collect all shards from sync.Map
	var shards []*hashShard
	h.shards.Range(func(key, value interface{}) bool {
		shards = append(shards, value.(*hashShard))
		return true
	})

	totalShards := len(shards)
	if totalShards == 0 {
		log.Printf("[HashIndex] No shards to finalize")
		return nil
	}

	// Close all writers first
	for _, shard := range shards {
		if err := shard.closeWriter(); err != nil {
			return fmt.Errorf("close shard %s writer: %w", shard.prefix, err)
		}
	}

	// Parallelize shard finalization with a worker pool
	numWorkers := 4 // Process 4 shards concurrently
	semaphore := make(chan struct{}, numWorkers)
	errChan := make(chan error, len(shards))
	var wg sync.WaitGroup
	var processed int32

	log.Printf("[HashIndex] Finalizing %d shards (workers=%d)...", totalShards, numWorkers)

	for _, shard := range shards {
		wg.Add(1)
		go func(s *hashShard) {
			defer wg.Done()
			semaphore <- struct{}{}        // Acquire
			defer func() { <-semaphore }() // Release

			log.Printf("[HashIndex] Building shard %s (entries=%d)...", s.prefix, s.count)
			payload, err := s.buildPayload()
			if err != nil {
				errChan <- fmt.Errorf("build shard %s payload: %w", s.prefix, err)
				return
			}

			objectPath := filepath.ToSlash(filepath.Join("hashmap", s.prefix+".shard"))
			writer := bucket.Object(objectPath).NewWriter(ctx)
			writer.ContentType = "application/octet-stream"
			if _, err := writer.Write(payload); err != nil {
				_ = writer.Close()
				errChan <- fmt.Errorf("write shard %s: %w", s.prefix, err)
				return
			}
			if err := writer.Close(); err != nil {
				errChan <- fmt.Errorf("close shard %s writer: %w", s.prefix, err)
				return
			}

			if cleanupFunc != nil {
				if err := cleanupFunc(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
					errChan <- fmt.Errorf("cleanup shard %s: %w", s.prefix, err)
					return
				}
			}

			n := atomic.AddInt32(&processed, 1)
			log.Printf("[HashIndex] Uploaded shard %s (%d/%d, %0.1f%%, bytes=%d)", s.prefix, n, totalShards, float64(n)*100.0/float64(totalShards), len(payload))
		}(shard)
	}

	// Wait for all workers to complete
	wg.Wait()
	close(errChan)

	// Check for any errors
	for err := range errChan {
		return err
	}

	return nil
}

func (h *hashIndexBuilder) getOrCreateShard(prefix string) (*hashShard, error) {
	// Fast path: shard already exists
	if existing, ok := h.shards.Load(prefix); ok {
		return existing.(*hashShard), nil
	}

	// Slow path: create new shard
	newShard, err := newHashShard(h.shardDir, prefix, shardSuffixByteLen, h.treeSize)
	if err != nil {
		return nil, err
	}

	// Try to store, or use existing if another goroutine created it first
	actual, loaded := h.shards.LoadOrStore(prefix, newShard)
	if loaded {
		// Another goroutine created it, close our duplicate
		_ = newShard.closeWriter()
	}
	return actual.(*hashShard), nil
}

func newHashShard(dir, prefix string, suffixLen int, treeSize uint64) (*hashShard, error) {
	path := filepath.Join(dir, fmt.Sprintf("%s.records", prefix))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open shard file: %w", err)
	}

	return &hashShard{
		prefix:    prefix,
		path:      path,
		file:      file,
		writer:    bufio.NewWriterSize(file, 8*1024), // 8KB buffer (reduced from 64KB to limit memory)
		suffixLen: suffixLen,
		treeSize:  treeSize,
	}, nil
}

func (s *hashShard) appendRecord(suffix []byte, index uint64) error {
	if len(suffix) != s.suffixLen {
		return fmt.Errorf("suffix length = %d, want %d", len(suffix), s.suffixLen)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.writer == nil {
		return errors.New("shard writer is closed")
	}

	// Combine suffix and index into single write to reduce syscalls
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], index)

	if _, err := s.writer.Write(suffix); err != nil {
		return fmt.Errorf("write suffix: %w", err)
	}
	if _, err := s.writer.Write(buf[:]); err != nil {
		return fmt.Errorf("write index: %w", err)
	}

	s.count++
	return nil
}

func (s *hashShard) closeWriter() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.writer == nil {
		return nil
	}

	// Flush buffered data first
	if err := s.writer.Flush(); err != nil {
		_ = s.file.Close()
		return fmt.Errorf("flush shard buffer: %w", err)
	}

	if err := s.file.Sync(); err != nil {
		_ = s.file.Close()
		return fmt.Errorf("sync shard file: %w", err)
	}
	if err := s.file.Close(); err != nil {
		return fmt.Errorf("close shard file: %w", err)
	}
	s.writer = nil
	s.file = nil
	return nil
}

func (s *hashShard) buildPayload() ([]byte, error) {
	file, err := os.Open(s.path)
	if err != nil {
		return nil, fmt.Errorf("open shard records: %w", err)
	}
	defer func() {
		_ = file.Close()
	}()

	if s.count == 0 {
		return nil, errors.New("shard contains no entries")
	}

	// Use buffered reader for better I/O performance
	reader := bufio.NewReaderSize(file, 256*1024) // 256KB buffer

	// Decide index width once based on overall tree size
	use32 := s.treeSize > 0 && (s.treeSize-1) <= math.MaxUint32

	suffixes := make([][]byte, s.count)
	var indices32 []uint32
	var indices64 []uint64
	if use32 {
		indices32 = make([]uint32, s.count)
	} else {
		indices64 = make([]uint64, s.count)
	}

	var indexBuf [8]byte
	for i := 0; i < s.count; i++ {
		suffix := make([]byte, s.suffixLen)
		if _, err := io.ReadFull(reader, suffix); err != nil {
			return nil, fmt.Errorf("read suffix %d: %w", i, err)
		}
		if _, err := io.ReadFull(reader, indexBuf[:]); err != nil {
			return nil, fmt.Errorf("read index %d: %w", i, err)
		}

		suffixes[i] = suffix
		idx := binary.BigEndian.Uint64(indexBuf[:])
		if use32 {
			indices32[i] = uint32(idx)
		} else {
			indices64[i] = idx
		}
	}

	// Build hashes and keys in single pass
	hashes := make([]uint64, s.count)
	keys := make([]string, s.count)
	for i := range suffixes {
		hashes[i] = xxhash.Sum64(suffixes[i])
		keys[i] = string(suffixes[i])
	}

	filter, err := xorfilter.PopulateBinaryFuse8(hashes)
	if err != nil {
		return nil, fmt.Errorf("build binary fuse filter: %w", err)
	}

	table := mph.New(keys)
	if table == nil {
		return nil, errors.New("mph construction failed")
	}

	shard := &hashmap.Shard{
		Prefix: s.prefix,
		Filter: filter,
		Mph:    table,
	}
	if use32 {
		shard.Indices32 = indices32
	} else {
		shard.Indices64 = indices64
	}

	payload, err := shard.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal shard: %w", err)
	}

	return payload, nil
}
