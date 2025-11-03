package hashmap

//go:generate protoc -I . --go_out=paths=source_relative:. pb/shard.proto

import (
	"errors"
	"fmt"
	"math"

	"github.com/FastFilter/xorfilter"
	hashmappb "github.com/bobcallaway/amber/internal/pkg/hashmap/pb"
	mph "github.com/dgryski/go-mph"
	"google.golang.org/protobuf/proto"
)

var (
	errUnsupportedVersion = errors.New("hash shard format version unsupported")
	errMismatchedLengths  = errors.New("hash shard payload length mismatch")
	errMissingIndices     = errors.New("hash shard missing indices")
)

const shardSuffixLengthBytes = 32 - (ShardPrefixLengthHex / 2)

// Shard encapsulates the deserialized shard artifacts used to satisfy
// GetInclusionProofByHash requests at runtime.
//
// The Fingerprints slice is shared with the underlying xorfilter data
// structure so callers must treat Shard as read-only once constructed.
type Shard struct {
	Prefix string

	Filter *xorfilter.BinaryFuse8
	Mph    *mph.Table

	Indices32 []uint32
	Indices64 []uint64
}

// Marshal encodes the shard into the canonical binary representation that is
// uploaded to object storage.
func (s *Shard) Marshal() ([]byte, error) {
	if s.Filter == nil {
		return nil, errors.New("nil binary fuse filter")
	}
	if s.Mph == nil {
		return nil, errors.New("nil minimal perfect hash table")
	}

	totalIndices := len(s.Indices32)
	if totalIndices == 0 {
		totalIndices = len(s.Indices64)
	}
	if totalIndices == 0 {
		return nil, errors.New("hash shard must contain at least one entry")
	}
	if totalIndices > math.MaxUint32 {
		return nil, fmt.Errorf("hash shard entry count %d exceeds supported limit", totalIndices)
	}

	if len(s.Indices32) > 0 && len(s.Indices64) > 0 {
		return nil, errors.New("hash shard indices must be consistently sized")
	}

	if len(s.Mph.Values) == 0 || len(s.Mph.Seeds) == 0 {
		return nil, errors.New("mph table incomplete")
	}
	if len(s.Mph.Values) != len(s.Mph.Seeds) {
		return nil, fmt.Errorf("mph table arrays mismatched: values=%d seeds=%d", len(s.Mph.Values), len(s.Mph.Seeds))
	}

	payload := &hashmappb.ShardPayload{
		Version:                  shardFormatVersion,
		PrefixLengthHex:          uint32(ShardPrefixLengthHex),
		SuffixLengthBytes:        uint32(shardSuffixLengthBytes),
		TotalEntries:             uint32(totalIndices),
		FilterSeed:               s.Filter.Seed,
		FilterSegmentLength:      s.Filter.SegmentLength,
		FilterSegmentLengthMask:  s.Filter.SegmentLengthMask,
		FilterSegmentCount:       s.Filter.SegmentCount,
		FilterSegmentCountLength: s.Filter.SegmentCountLength,
		FilterFingerprints:       append([]byte(nil), s.Filter.Fingerprints...),
		MphValues:                append([]int32(nil), s.Mph.Values...),
		MphSeeds:                 append([]int32(nil), s.Mph.Seeds...),
	}

	switch {
	case len(s.Indices32) > 0:
		payload.Indices = &hashmappb.ShardPayload_Indices32{
			Indices32: &hashmappb.Uint32List{Values: append([]uint32(nil), s.Indices32...)},
		}
	case len(s.Indices64) > 0:
		payload.Indices = &hashmappb.ShardPayload_Indices64{
			Indices64: &hashmappb.Uint64List{Values: append([]uint64(nil), s.Indices64...)},
		}
	default:
		return nil, errors.New("hash shard indices must be provided")
	}

	bytes, err := proto.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal shard payload: %w", err)
	}
	return bytes, nil
}

// UnmarshalShard decodes the shard payload and constructs an in-memory Shard
// instance ready for lookups.
func UnmarshalShard(prefix string, payloadBytes []byte) (*Shard, error) {
	msg := &hashmappb.ShardPayload{}
	if err := proto.Unmarshal(payloadBytes, msg); err != nil {
		return nil, fmt.Errorf("decode shard payload: %w", err)
	}

	if msg.GetVersion() != shardFormatVersion {
		return nil, errUnsupportedVersion
	}
	if int(msg.GetPrefixLengthHex()) != ShardPrefixLengthHex {
		return nil, fmt.Errorf("shard prefix length mismatch: got %d want %d", msg.GetPrefixLengthHex(), ShardPrefixLengthHex)
	}
	if int(msg.GetSuffixLengthBytes()) != shardSuffixLengthBytes {
		return nil, fmt.Errorf("shard suffix length mismatch: got %d want %d", msg.GetSuffixLengthBytes(), shardSuffixLengthBytes)
	}

	totalEntries := msg.GetTotalEntries()
	if totalEntries == 0 {
		return nil, errors.New("hash shard contains zero entries")
	}

	if len(msg.GetMphValues()) == 0 || len(msg.GetMphSeeds()) == 0 {
		return nil, errors.New("mph table incomplete")
	}
	if len(msg.GetMphValues()) != len(msg.GetMphSeeds()) {
		return nil, errMismatchedLengths
	}
	if uint32(len(msg.GetMphValues())) < totalEntries {
		return nil, errMismatchedLengths
	}

	filter := &xorfilter.BinaryFuse8{
		Seed:               msg.GetFilterSeed(),
		SegmentLength:      uint32(msg.GetFilterSegmentLength()),
		SegmentLengthMask:  uint32(msg.GetFilterSegmentLengthMask()),
		SegmentCount:       uint32(msg.GetFilterSegmentCount()),
		SegmentCountLength: uint32(msg.GetFilterSegmentCountLength()),
		Fingerprints:       append([]uint8(nil), msg.GetFilterFingerprints()...),
	}

	table := &mph.Table{
		Values: append([]int32(nil), msg.GetMphValues()...),
		Seeds:  append([]int32(nil), msg.GetMphSeeds()...),
	}

	shard := &Shard{
		Prefix: prefix,
		Filter: filter,
		Mph:    table,
	}

	switch idx := msg.GetIndices().(type) {
	case *hashmappb.ShardPayload_Indices32:
		values := idx.Indices32.GetValues()
		if uint32(len(values)) != totalEntries {
			return nil, errMismatchedLengths
		}
		shard.Indices32 = append([]uint32(nil), values...)
	case *hashmappb.ShardPayload_Indices64:
		values := idx.Indices64.GetValues()
		if uint32(len(values)) != totalEntries {
			return nil, errMismatchedLengths
		}
		shard.Indices64 = append([]uint64(nil), values...)
	default:
		return nil, errMissingIndices
	}

	return shard, nil
}

// Lookup resolves the global log index for the provided suffix. The caller is
// responsible for hashing the suffix with the same hash function used during
// construction before calling ContainsHash.
func (s *Shard) Lookup(suffix []byte) (uint64, bool) {
	idx := s.Mph.Query(string(suffix))
	if idx < 0 {
		return 0, false
	}

	if len(s.Indices64) > 0 {
		if int(idx) >= len(s.Indices64) {
			return 0, false
		}
		return s.Indices64[idx], true
	}

	if int(idx) >= len(s.Indices32) {
		return 0, false
	}
	return uint64(s.Indices32[idx]), true
}

// ContainsHash reports whether the hashed suffix is probably a member of the
// shard using the binary fuse filter.
func (s *Shard) ContainsHash(hash uint64) bool {
	return s.Filter.Contains(hash)
}

// EntryCount returns the number of elements encoded in the shard.
func (s *Shard) EntryCount() int {
	if len(s.Indices64) > 0 {
		return len(s.Indices64)
	}
	return len(s.Indices32)
}
