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

package hashmap

import "math"

// CalculateShardPrefixLength determines the hexadecimal prefix length to use when
// sharding hash-index files. The result is rounded to the nearest integer using
// "round half up" semantics applied to log base 256 of the tree size. The value
// is clamped to the inclusive range [1, 64] to match the length of a SHA-256
// digest represented in hex.
func CalculateShardPrefixLength(treeSize uint64) uint {
	if treeSize == 0 {
		return 1
	}

	logValue := math.Log(float64(treeSize)) / math.Log(256)
	rounded := min(max(int(math.Floor(logValue+0.5)), 1), 64)

	return uint(rounded)
}
