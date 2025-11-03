package api

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/FastFilter/xorfilter"
	"github.com/bobcallaway/amber/internal/pkg/hashmap"
	"github.com/cespare/xxhash/v2"
	mph "github.com/dgryski/go-mph"
	lru "github.com/hashicorp/golang-lru/v2"
	f_log "github.com/transparency-dev/formats/log"
)

type fakeHashAccessor struct {
	payloads map[string][]byte
}

func (f *fakeHashAccessor) HashPrefixExists(_ context.Context, prefix string) (bool, error) {
	_, ok := f.payloads[prefix]
	return ok, nil
}

func (f *fakeHashAccessor) ReadShard(_ context.Context, prefix string) ([]byte, error) {
	payload, ok := f.payloads[prefix]
	if !ok {
		return nil, fmt.Errorf("shard %s: %w", prefix, os.ErrNotExist)
	}
	return payload, nil
}

func makeShardPayload(t *testing.T, suffixes [][]byte, indices []uint64) []byte {
	t.Helper()

	hashes := make([]uint64, len(suffixes))
	keys := make([]string, len(suffixes))
	for i := range suffixes {
		hashes[i] = xxhash.Sum64(suffixes[i])
		keys[i] = string(suffixes[i])
	}

	filter, err := xorfilter.PopulateBinaryFuse8(hashes)
	if err != nil {
		t.Fatalf("build filter: %v", err)
	}

	table := mph.New(keys)
	if table == nil {
		t.Fatalf("mph construction failed")
	}

	shard := &hashmap.Shard{
		Filter:    filter,
		Mph:       table,
		Indices64: indices,
	}

	payload, err := shard.Marshal()
	if err != nil {
		t.Fatalf("marshal shard: %v", err)
	}
	return payload
}

func makeServerForTest(t *testing.T, cpSize uint64, accessor hashShardAccessor) *trillianLogServer {
	cache, err := lru.New[string, *hashmap.Shard](4)
	if err != nil {
		t.Fatalf("create cache: %v", err)
	}

	prefixBytes := int(hashmap.ShardPrefixLengthHex) / 2

	return &trillianLogServer{
		cp:            f_log.Checkpoint{Size: cpSize},
		hashShards:    accessor,
		shardCache:    cache,
		prefixByteLen: prefixBytes,
		suffixByteLen: 32 - prefixBytes,
	}
}

func TestLookupLeafIndexByHashFound(t *testing.T) {
	digestHex := strings.Repeat("ab", 32)
	leafHash, err := hex.DecodeString(digestHex)
	if err != nil {
		t.Fatalf("decode digest: %v", err)
	}

	prefixHex := digestHex[:hashmap.ShardPrefixLengthHex]
	suffixHex := digestHex[hashmap.ShardPrefixLengthHex:]
	suffix, err := hex.DecodeString(suffixHex)
	if err != nil {
		t.Fatalf("decode suffix: %v", err)
	}

	const wantIndex = uint64(123456)
	payload := makeShardPayload(t, [][]byte{suffix}, []uint64{wantIndex})

	accessor := &fakeHashAccessor{
		payloads: map[string][]byte{prefixHex: payload},
	}

	server := makeServerForTest(t, 1<<20, accessor)

	got, err := server.lookupLeafIndexByHash(context.Background(), leafHash)
	if err != nil {
		t.Fatalf("lookupLeafIndexByHash: %v", err)
	}
	if uint64(got) != wantIndex {
		t.Fatalf("lookupLeafIndexByHash = %d, want %d", got, wantIndex)
	}
}

func TestLookupLeafIndexByHashPrefixMiss(t *testing.T) {
	digestHex := strings.Repeat("01", 32)
	leafHash, err := hex.DecodeString(digestHex)
	if err != nil {
		t.Fatalf("decode digest: %v", err)
	}

	accessor := &fakeHashAccessor{payloads: map[string][]byte{}}
	server := makeServerForTest(t, 4096, accessor)

	_, err = server.lookupLeafIndexByHash(context.Background(), leafHash)
	if !errors.Is(err, errHashIndexNotFound) {
		t.Fatalf("lookupLeafIndexByHash error = %v, want errHashIndexNotFound", err)
	}
}

func TestLookupLeafIndexByHashFilterNegative(t *testing.T) {
	digestHex := strings.Repeat("02", 32)
	leafHash, err := hex.DecodeString(digestHex)
	if err != nil {
		t.Fatalf("decode digest: %v", err)
	}

	prefixHex := digestHex[:hashmap.ShardPrefixLengthHex]
	otherHex := strings.Repeat("03", 32)
	otherSuffix, err := hex.DecodeString(otherHex[hashmap.ShardPrefixLengthHex:])
	if err != nil {
		t.Fatalf("decode other suffix: %v", err)
	}

	payload := makeShardPayload(t, [][]byte{otherSuffix}, []uint64{42})
	accessor := &fakeHashAccessor{payloads: map[string][]byte{prefixHex: payload}}
	server := makeServerForTest(t, 1<<12, accessor)

	_, err = server.lookupLeafIndexByHash(context.Background(), leafHash)
	if !errors.Is(err, errHashIndexNotFound) {
		t.Fatalf("lookupLeafIndexByHash error = %v, want errHashIndexNotFound", err)
	}
}

func TestLookupLeafIndexByHashCorruptShard(t *testing.T) {
	digestHex := strings.Repeat("04", 32)
	leafHash, err := hex.DecodeString(digestHex)
	if err != nil {
		t.Fatalf("decode digest: %v", err)
	}

	prefixHex := digestHex[:hashmap.ShardPrefixLengthHex]
	accessor := &fakeHashAccessor{payloads: map[string][]byte{prefixHex: []byte("not-a-shard")}}
	server := makeServerForTest(t, 1<<8, accessor)

	_, err = server.lookupLeafIndexByHash(context.Background(), leafHash)
	if !errors.Is(err, errHashShardCorrupt) {
		t.Fatalf("lookupLeafIndexByHash error = %v, want errHashShardCorrupt", err)
	}
}
