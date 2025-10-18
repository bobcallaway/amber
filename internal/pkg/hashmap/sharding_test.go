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

import "testing"

func TestCalculateShardPrefixLength(t *testing.T) {
	tests := []struct {
		name     string
		treeSize uint64
		want     uint
	}{
		{"zero defaults to one", 0, 1},
		{"single leaf", 1, 1},
		{"less than half step", 255, 1},
		{"exact power of 256", 256, 1},
		{"next power", 256 * 256, 2},
		{"rounds down below half", 1048576 - 1, 2},
		{"rounds up at half", 1048576, 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := CalculateShardPrefixLength(tc.treeSize); got != tc.want {
				t.Fatalf("CalculateShardPrefixLength(%d) = %d, want %d", tc.treeSize, got, tc.want)
			}
		})
	}
}
