package api

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/bits-and-blooms/bloom"
	"github.com/bobcallaway/amber/internal/pkg/hashmap"
	f_log "github.com/transparency-dev/formats/log"
	bolt "go.etcd.io/bbolt"
)

type fakeHashAccessor struct {
	prefixes map[string]struct{}
	objects  map[string][]byte
}

func (f *fakeHashAccessor) HashPrefixExists(_ context.Context, prefix string) (bool, error) {
	_, ok := f.prefixes[prefix]
	return ok, nil
}

func (f *fakeHashAccessor) HashShardPaths(prefix string) (string, string) {
	return fmt.Sprintf("hashmap/%s.bloom", prefix), fmt.Sprintf("hashmap/%s.db", prefix)
}

func (f *fakeHashAccessor) ReadObject(_ context.Context, path string) ([]byte, error) {
	if data, ok := f.objects[path]; ok {
		return data, nil
	}
	return nil, fmt.Errorf("object %s: %w", path, os.ErrNotExist)
}

func createBloomBytes(t *testing.T, suffix string, add bool) []byte {
	t.Helper()

	filter := bloom.NewWithEstimates(16, 0.01)
	if add {
		filter.Add([]byte(suffix))
	}

	var buf bytes.Buffer
	if _, err := filter.WriteTo(&buf); err != nil {
		t.Fatalf("write bloom filter: %v", err)
	}

	return buf.Bytes()
}

func createBoltBytes(t *testing.T, suffix string, add bool, includeBucket bool, index uint64) []byte {
	t.Helper()

	tmp, err := os.CreateTemp("", "hashdb-*.db")
	if err != nil {
		t.Fatalf("create temp db: %v", err)
	}
	tmpName := tmp.Name()
	if err := tmp.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}
	defer os.Remove(tmpName)

	opts := &bolt.Options{Timeout: 0}
	db, err := bolt.Open(tmpName, 0o600, opts)
	if err != nil {
		t.Fatalf("open bolt db: %v", err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		if !includeBucket {
			return nil
		}
		bucket, err := tx.CreateBucketIfNotExists(hashmap.HashIndexBucketKey)
		if err != nil {
			return err
		}
		if add {
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], index)
			if err := bucket.Put([]byte(suffix), buf[:]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		db.Close()
		t.Fatalf("populate bolt db: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close bolt db: %v", err)
	}

	contents, err := os.ReadFile(tmpName)
	if err != nil {
		t.Fatalf("read bolt db: %v", err)
	}

	return contents
}

func makeServerForTest(cpSize uint64, accessor hashShardAccessor) *trillianLogServer {
	return &trillianLogServer{
		cp:         f_log.Checkpoint{Size: cpSize},
		hashShards: accessor,
	}
}

func TestLookupLeafIndexByHashFound(t *testing.T) {
	leafHash := bytes.Repeat([]byte{0xAB}, 32)
	hexDigest := fmt.Sprintf("%x", leafHash)
	cpSize := uint64(65536)
	prefixLen := hashmap.CalculateShardPrefixLength(cpSize)
	prefix := hexDigest[:int(prefixLen)]
	suffix := hexDigest[int(prefixLen):]
	const wantIndex = int64(123456)

	bloomBytes := createBloomBytes(t, suffix, true)
	dbBytes := createBoltBytes(t, suffix, true, true, uint64(wantIndex))

	accessor := &fakeHashAccessor{
		prefixes: map[string]struct{}{prefix: {}},
		objects: map[string][]byte{
			fmt.Sprintf("hashmap/%s.bloom", prefix): bloomBytes,
			fmt.Sprintf("hashmap/%s.db", prefix):    dbBytes,
		},
	}

	server := makeServerForTest(cpSize, accessor)

	got, err := server.lookupLeafIndexByHash(context.Background(), leafHash)
	if err != nil {
		t.Fatalf("lookupLeafIndexByHash: %v", err)
	}
	if got != wantIndex {
		t.Fatalf("lookupLeafIndexByHash = %d, want %d", got, wantIndex)
	}
}

func TestLookupLeafIndexByHashPrefixMiss(t *testing.T) {
	leafHash := bytes.Repeat([]byte{0x01}, 32)
	cpSize := uint64(1024)

	accessor := &fakeHashAccessor{
		prefixes: map[string]struct{}{},
		objects:  map[string][]byte{},
	}

	server := makeServerForTest(cpSize, accessor)

	_, err := server.lookupLeafIndexByHash(context.Background(), leafHash)
	if !errors.Is(err, errHashIndexNotFound) {
		t.Fatalf("lookupLeafIndexByHash error = %v, want errHashIndexNotFound", err)
	}
}

func TestLookupLeafIndexByHashBloomNegative(t *testing.T) {
	leafHash := bytes.Repeat([]byte{0x02}, 32)
	hexDigest := fmt.Sprintf("%x", leafHash)
	cpSize := uint64(65536)
	prefixLen := hashmap.CalculateShardPrefixLength(cpSize)
	prefix := hexDigest[:int(prefixLen)]
	suffix := hexDigest[int(prefixLen):]

	bloomBytes := createBloomBytes(t, suffix, false)
	dbBytes := createBoltBytes(t, suffix, true, true, 42)

	accessor := &fakeHashAccessor{
		prefixes: map[string]struct{}{prefix: {}},
		objects: map[string][]byte{
			fmt.Sprintf("hashmap/%s.bloom", prefix): bloomBytes,
			fmt.Sprintf("hashmap/%s.db", prefix):    dbBytes,
		},
	}

	server := makeServerForTest(cpSize, accessor)

	_, err := server.lookupLeafIndexByHash(context.Background(), leafHash)
	if !errors.Is(err, errHashIndexNotFound) {
		t.Fatalf("lookupLeafIndexByHash error = %v, want errHashIndexNotFound", err)
	}
}

func TestLookupLeafIndexByHashDBMiss(t *testing.T) {
	leafHash := bytes.Repeat([]byte{0x03}, 32)
	hexDigest := fmt.Sprintf("%x", leafHash)
	cpSize := uint64(65536)
	prefixLen := hashmap.CalculateShardPrefixLength(cpSize)
	prefix := hexDigest[:int(prefixLen)]
	suffix := hexDigest[int(prefixLen):]

	bloomBytes := createBloomBytes(t, suffix, true)
	dbBytes := createBoltBytes(t, suffix, false, true, 88)

	accessor := &fakeHashAccessor{
		prefixes: map[string]struct{}{prefix: {}},
		objects: map[string][]byte{
			fmt.Sprintf("hashmap/%s.bloom", prefix): bloomBytes,
			fmt.Sprintf("hashmap/%s.db", prefix):    dbBytes,
		},
	}

	server := makeServerForTest(cpSize, accessor)

	_, err := server.lookupLeafIndexByHash(context.Background(), leafHash)
	if !errors.Is(err, errHashIndexNotFound) {
		t.Fatalf("lookupLeafIndexByHash error = %v, want errHashIndexNotFound", err)
	}
}

func TestLookupLeafIndexByHashCorruptBloom(t *testing.T) {
	leafHash := bytes.Repeat([]byte{0x04}, 32)
	hexDigest := fmt.Sprintf("%x", leafHash)
	cpSize := uint64(65536)
	prefixLen := hashmap.CalculateShardPrefixLength(cpSize)
	prefix := hexDigest[:int(prefixLen)]

	accessor := &fakeHashAccessor{
		prefixes: map[string]struct{}{prefix: {}},
		objects: map[string][]byte{
			fmt.Sprintf("hashmap/%s.bloom", prefix): []byte{0x00},
		},
	}

	server := makeServerForTest(cpSize, accessor)

	_, err := server.lookupLeafIndexByHash(context.Background(), leafHash)
	if !errors.Is(err, errBloomFilterCorrupt) {
		t.Fatalf("lookupLeafIndexByHash error = %v, want errBloomFilterCorrupt", err)
	}
}

func TestLookupLeafIndexByHashCorruptDB(t *testing.T) {
	leafHash := bytes.Repeat([]byte{0x05}, 32)
	hexDigest := fmt.Sprintf("%x", leafHash)
	cpSize := uint64(65536)
	prefixLen := hashmap.CalculateShardPrefixLength(cpSize)
	prefix := hexDigest[:int(prefixLen)]
	suffix := hexDigest[int(prefixLen):]

	bloomBytes := createBloomBytes(t, suffix, true)
	dbBytes := createBoltBytes(t, suffix, true, false, 99)

	accessor := &fakeHashAccessor{
		prefixes: map[string]struct{}{prefix: {}},
		objects: map[string][]byte{
			fmt.Sprintf("hashmap/%s.bloom", prefix): bloomBytes,
			fmt.Sprintf("hashmap/%s.db", prefix):    dbBytes,
		},
	}

	server := makeServerForTest(cpSize, accessor)

	_, err := server.lookupLeafIndexByHash(context.Background(), leafHash)
	if !errors.Is(err, errHashDatabaseCorrupt) {
		t.Fatalf("lookupLeafIndexByHash error = %v, want errHashDatabaseCorrupt", err)
	}
}
