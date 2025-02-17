- finish gRPC start & listener
- trace/span
- structured logging
- implement bolt DB lookup from hash -> index
- support static timestamp per log for STH?
 - could modify config map[string]struct for this
- return status.Error from all paths to be consistent with codes

Test harness
- Launch kind cluster
- apply deployment (traditional trillian/memory stack)
- write 300 entries into log 
- launch GCS emulator, expose as service
- launch amber-polymerize job
  - this should do a full copy out from the trillian-memory instance into GCS
  - this quantity of entries should complete a single tile and also write a partial tile
- write amber-server config file into ConfigMap
- launch amber-server

- write harness/use grpc_cli to hit actual and amber-server trillian endpoints to compare and contrast:
  - get GetEntryAndProof from all entries 0-299, ensuring results are identical
  - get GetLeavesByRange for all ranges [0-299] and ensure they are identical
  - get ConsistencyProofs for all ranges [0-299] and ensure they are identical
  - get InclusionProof (by index) for all entries and ensure they're identical
  - get InclusionProof (by hash) for all entries and ensure they're identical
  - Error conditions:
    - GetEntryAndProof for index outside of tree
    - GetInclusionProof (by index) for index outside of tree
    - GetInclusionProofByHash for hash not in tree
    - GetConsistencyProof for range outside of tree
    - GetLeavesByRange for range outside of tree

package main

import "fmt"

func generateSequentialRanges(start, end int) [][]int {
	var ranges [][]int

	for i := start; i <= end; i++ {
		for j := i + 1; j <= end; j++ {
			ranges = append(ranges, generateRange(i, j))
		}
	}
	return ranges
}

func generateRange(start, end int) []int {
	rangeSlice := make([]int, end-start+1)
	for i := start; i <= end; i++ {
		rangeSlice[i-start] = i
	}
	return rangeSlice
}

func main() {
	// Generate all sequential ranges from 0 to 299
	ranges := generateSequentialRanges(0, 299)

	// Print all ranges
	for _, r := range ranges {
		fmt.Println(r)
	}
}
