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
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/bobcallaway/amber/pkg/config"
	"github.com/google/trillian"
	"github.com/google/trillian/types"
	f_log "github.com/transparency-dev/formats/log"
	"github.com/transparency-dev/merkle/rfc6962"
	t_api "github.com/transparency-dev/trillian-tessera/api"
	"github.com/transparency-dev/trillian-tessera/api/layout"
	tessera "github.com/transparency-dev/trillian-tessera/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type logMap struct {
	mu   sync.RWMutex
	data map[int64]*trillianLogServer
}

func newLogMap() *logMap {
	return &logMap{
		data: make(map[int64]*trillianLogServer),
	}
}

func (l *logMap) Add(logID, frozenTime int64, bucketName string) error {
	fetcher, err := NewGCSFetcher(bucketName)
	if err != nil {
		return err
	}

	cpRaw, err := fetcher.ReadCheckpoint(context.Background())
	if err != nil {
		return err
	}
	cp := f_log.Checkpoint{}
	if _, err := cp.Unmarshal(cpRaw); err != nil {
		return err
	}

	pb, err := tessera.NewProofBuilder(context.Background(), cp, fetcher.ReadTile)
	if err != nil {
		return err
	}
	server := &trillianLogServer{
		f:     fetcher,
		pb:    pb,
		cp:    cp,
		ts:    frozenTime,
		logID: logID,
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.data[logID]; !ok {
		l.data[logID] = server
	}
	return nil
}

func (l *logMap) Get(logID int64) (*trillianLogServer, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	server, ok := l.data[logID]
	return server, ok
}

type Facade struct {
	logMap *logMap
}

func NewFacade(config *config.Config) (*Facade, error) {
	facade := Facade{
		logMap: newLogMap(),
	}
	for k, v := range config.LogConfigs {
		if err := facade.logMap.Add(k, v.FrozenTime, v.BucketName); err != nil {
			return nil, err
		}
	}
	return &facade, nil
}

func (f *Facade) GetInclusionProof(ctx context.Context, req *trillian.GetInclusionProofRequest) (*trillian.GetInclusionProofResponse, error) {
	t, ok := f.logMap.Get(req.LogId)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "service is not configured for requested log ID %d", req.LogId)
	}

	// validate request
	if req.TreeSize <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "GetInclusionProofRequest.TreeSize: %v, want > 0", req.TreeSize)
	}
	if req.LeafIndex < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "GetInclusionProofRequest.LeafIndex: %v, want >= 0", req.LeafIndex)
	}
	if req.LeafIndex >= req.TreeSize {
		return nil, status.Errorf(codes.InvalidArgument, "GetInclusionProofRequest.LeafIndex: %v >= TreeSize: %v, want < ", req.LeafIndex, req.TreeSize)
	}

	slr, err := t.getSignedLogRoot()
	if err != nil {
		return nil, err
	}

	// ensure that requested size is <= known size of log
	if req.TreeSize > int64(t.cp.Size) {
		// return an empty proof and a SignedLogRoot that includes the tree size we know about
		return &trillian.GetInclusionProofResponse{
			SignedLogRoot: slr,
		}, nil
	}

	proof, err := t.pb.InclusionProof(ctx, uint64(req.LeafIndex))
	if err != nil {
		return nil, err
	}
	resp := &trillian.GetInclusionProofResponse{
		Proof: &trillian.Proof{
			Hashes:    proof,
			LeafIndex: req.LeafIndex,
		},
		SignedLogRoot: slr,
	}
	return resp, nil
}

func (f *Facade) GetInclusionProofByHash(ctx context.Context, req *trillian.GetInclusionProofByHashRequest) (*trillian.GetInclusionProofByHashResponse, error) {
	t, ok := f.logMap.Get(req.LogId)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "service is not configured for requested log ID %d", req.LogId)
	}

	// validate request
	if req.TreeSize <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "GetInclusionProofByHashRequest.TreeSize: %v, want > 0", req.TreeSize)
	}
	if got, want := len(req.LeafHash), rfc6962.DefaultHasher.Size(); got != want {
		return nil, status.Errorf(codes.InvalidArgument, "GetInclusionProofByHashRequest.LeafHash was %d bytes, wanted %d", got, want)
	}

	// TODO: Lookup index for leaf hash from bolt db file
	var index int64

	wrappedResp, err := f.GetInclusionProof(ctx, &trillian.GetInclusionProofRequest{
		LogId:     req.LogId,
		LeafIndex: index,
	})
	if err != nil {
		return nil, err
	}

	slr, err := t.getSignedLogRoot()
	if err != nil {
		return nil, err
	}
	// translate between trillian response message types
	resp := &trillian.GetInclusionProofByHashResponse{
		Proof:         []*trillian.Proof{wrappedResp.Proof},
		SignedLogRoot: slr,
	}
	return resp, nil
}

func (f *Facade) GetConsistencyProof(ctx context.Context, req *trillian.GetConsistencyProofRequest) (*trillian.GetConsistencyProofResponse, error) {
	t, ok := f.logMap.Get(req.LogId)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "service is not configured for requested log ID %d", req.LogId)
	}

	// validate request
	if req.FirstTreeSize <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "GetConsistencyProofRequest.FirstTreeSize: %v, want > 0", req.FirstTreeSize)
	}
	if req.SecondTreeSize <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "GetConsistencyProofRequest.SecondTreeSize: %v, want > 0", req.SecondTreeSize)
	}
	if req.SecondTreeSize < req.FirstTreeSize {
		return nil, status.Errorf(codes.InvalidArgument, "GetConsistencyProofRequest.SecondTreeSize: %v < GetConsistencyProofRequest.FirstTreeSize: %v, want >= ", req.SecondTreeSize, req.FirstTreeSize)
	}

	proof, err := t.pb.ConsistencyProof(ctx, uint64(req.FirstTreeSize), uint64(req.SecondTreeSize))
	if err != nil {
		return nil, err
	}

	slr, err := t.getSignedLogRoot()
	if err != nil {
		return nil, err
	}
	resp := &trillian.GetConsistencyProofResponse{
		Proof: &trillian.Proof{
			Hashes:    proof,
			LeafIndex: 0, // this is hard coded to 0 for consistency proofs
		},
		SignedLogRoot: slr,
	}
	return resp, nil
}

func (f *Facade) GetLatestSignedLogRoot(ctx context.Context, req *trillian.GetLatestSignedLogRootRequest) (*trillian.GetLatestSignedLogRootResponse, error) {
	t, ok := f.logMap.Get(req.LogId)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "service is not configured for requested log ID %d", req.LogId)
	}

	slr, err := t.getSignedLogRoot()
	if err != nil {
		return nil, err
	}

	resp := &trillian.GetLatestSignedLogRootResponse{SignedLogRoot: slr}

	var proof [][]byte
	// get consistency proof from 0 to current size
	if req.FirstTreeSize != 0 && req.FirstTreeSize <= int64(t.cp.Size) {
		proof, err = t.pb.ConsistencyProof(ctx, uint64(req.FirstTreeSize), t.cp.Size)
		if err != nil {
			return nil, err
		}
		resp.Proof = &trillian.Proof{
			LeafIndex: 0, // this is hard coded to 0 for consistency proofs
			Hashes:    proof,
		}
	}

	return resp, nil
}

func (f *Facade) GetEntryAndProof(ctx context.Context, req *trillian.GetEntryAndProofRequest) (*trillian.GetEntryAndProofResponse, error) {
	t, ok := f.logMap.Get(req.LogId)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "service is not configured for requested log ID %d", req.LogId)
	}

	// validate request
	if req.TreeSize <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "GetEntryAndProofRequest.TreeSize: %v, want > 0", req.TreeSize)
	}
	if req.LeafIndex < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "GetEntryAndProofRequest.LeafIndex: %v, want >= 0", req.LeafIndex)
	}
	if req.LeafIndex >= req.TreeSize {
		return nil, status.Errorf(codes.InvalidArgument, "GetEntryAndProofRequest.LeafIndex: %v >= TreeSize: %v, want < ", req.LeafIndex, req.TreeSize)
	}

	// compute path of object to read out of bucket
	path := layout.EntriesPathForLogIndex(uint64(req.LeafIndex), t.cp.Size)
	entries, metadata, err := t.f.FetchWithMetadata(ctx, path)
	if err != nil {
		return nil, err
	}

	bundle := t_api.EntryBundle{}
	if err := bundle.UnmarshalText(entries); err != nil {
		return nil, err
	}

	// find offset within bundle to know which entry to return
	offset := req.LeafIndex % layout.TileWidth
	entry := bundle.Entries[offset]

	// extract timestamps for entry from object metadata
	offsetB36 := big.NewInt(offset).Text(36)
	queueTSB36, integrateOffsetTSB36, found := strings.Cut(metadata[offsetB36], "")
	if !found {
		return nil, errors.New("unable to parse timestamp metadata")
	}
	queueTS := new(big.Int)
	if _, ok := queueTS.SetString(queueTSB36, 36); !ok {
		return nil, errors.New("unable to parse queue timestamp")
	}
	integrateOffsetTS := new(big.Int)
	if _, ok := integrateOffsetTS.SetString(integrateOffsetTSB36, 36); !ok {
		return nil, errors.New("unable to parse integration timestamp offset")
	}

	// get inclusion proof for entry
	proof, err := t.pb.InclusionProof(ctx, uint64(req.LeafIndex))
	if err != nil {
		return nil, err
	}

	slr, err := t.getSignedLogRoot()
	if err != nil {
		return nil, err
	}

	resp := &trillian.GetEntryAndProofResponse{
		Leaf: &trillian.LogLeaf{
			LeafIndex:          req.LeafIndex,
			LeafValue:          entry,
			MerkleLeafHash:     rfc6962.DefaultHasher.HashLeaf(entry),
			QueueTimestamp:     timestamppb.New(time.Unix(0, queueTS.Int64())),
			IntegrateTimestamp: timestamppb.New(time.Unix(0, queueTS.Int64()+integrateOffsetTS.Int64())),
		},
		Proof: &trillian.Proof{
			Hashes:    proof,
			LeafIndex: req.LeafIndex,
		},
		SignedLogRoot: slr,
	}

	return resp, nil
}

func (f *Facade) GetLeavesByRange(ctx context.Context, req *trillian.GetLeavesByRangeRequest) (*trillian.GetLeavesByRangeResponse, error) {
	t, ok := f.logMap.Get(req.LogId)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "service is not configured for requested log ID %d", req.LogId)
	}

	// validate request
	if req.StartIndex < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "GetLeavesByRangeRequest.StartIndex: %v, want >= 0", req.StartIndex)
	}
	if req.Count <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "GetLeavesByRangeRequest.Count: %v, want > 0", req.Count)
	}

	resp := &trillian.GetLeavesByRangeResponse{}

	// compute range of objects to read from GCS
	seqIterator := layout.Range(uint64(req.StartIndex), uint64(req.Count), t.cp.Size)
	for iter := range seqIterator {
		entries, metadata, err := t.f.ReadEntryBundleWithMetadata(ctx, iter.Index, iter.Partial)
		if err != nil {
			return nil, err
		}
		bundle := t_api.EntryBundle{}
		if err := bundle.UnmarshalText(entries); err != nil {
			return nil, err
		}
		for i := uint(0); i < iter.N; i++ {
			offsetB36 := big.NewInt(int64(iter.First + i)).Text(36)
			queueTSB36, integrateOffsetTSB36, found := strings.Cut(metadata[offsetB36], "")
			if !found {
				return nil, errors.New("unable to parse timestamp metadata")
			}
			queueTS := new(big.Int)
			if _, ok := queueTS.SetString(queueTSB36, 36); !ok {
				return nil, errors.New("unable to parse queue timestamp")
			}
			integrateOffsetTS := new(big.Int)
			if _, ok := integrateOffsetTS.SetString(integrateOffsetTSB36, 36); !ok {
				return nil, errors.New("unable to parse integration timestamp offset")
			}

			resp.Leaves = append(resp.Leaves, &trillian.LogLeaf{
				LeafValue:          bundle.Entries[iter.First+i],
				MerkleLeafHash:     rfc6962.DefaultHasher.HashLeaf(bundle.Entries[iter.First+i]),
				LeafIndex:          int64(iter.First + i),
				QueueTimestamp:     timestamppb.New(time.Unix(0, queueTS.Int64())),
				IntegrateTimestamp: timestamppb.New(time.Unix(0, queueTS.Int64()+integrateOffsetTS.Int64())),
			})
		}
	}

	slr, err := t.getSignedLogRoot()
	if err != nil {
		return nil, err
	}

	resp.SignedLogRoot = slr

	return resp, nil
}

type trillianLogServer struct {
	logID int64
	pb    *tessera.ProofBuilder
	f     *GCSFetcher
	cp    f_log.Checkpoint
	ts    int64
}

func (t *trillianLogServer) getSignedLogRoot() (*trillian.SignedLogRoot, error) {
	// this converts the checkpoint data to a STH, with the frozen time representing timestamp_nanos
	lr, err := (&types.LogRootV1{
		TreeSize:       t.cp.Size,
		RootHash:       t.cp.Hash,
		TimestampNanos: uint64(t.ts),
	}).MarshalBinary()
	if err != nil {
		return nil, err
	}
	return &trillian.SignedLogRoot{
		LogRoot: lr,
	}, nil
}

// Unimplemented gRPC service methods since this is for read-only usage
func (f *Facade) InitLog(_ context.Context, _ *trillian.InitLogRequest) (*trillian.InitLogResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "service is a read-only facade")
}

func (f *Facade) QueueLeaf(_ context.Context, _ *trillian.QueueLeafRequest) (*trillian.QueueLeafResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "service is a read-only facade")
}

func (f *Facade) AddSequencedLeaves(_ context.Context, _ *trillian.AddSequencedLeavesRequest) (*trillian.AddSequencedLeavesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "service is a read-only facade")
}
