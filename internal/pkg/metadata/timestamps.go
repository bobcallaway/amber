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

package metadata

import (
	"fmt"
	"math/big"
	"strings"
)

// TimestampPair represents a queue timestamp and integration timestamp.
type TimestampPair struct {
	QueueTimestampNanos     int64
	IntegrateTimestampNanos int64
}

// BuildTimestampMetadata creates a map of base-36 encoded metadata for an entry bundle.
// The keys are base-36 encoded indices within the bundle (0-255).
// The values are space-separated base-36 encoded queue timestamp and integration offset.
//
// This encoding allows storing timestamp data for up to 256 entries within GCS's
// 8 KiB metadata limit (uses ~7.2 KiB max).
func BuildTimestampMetadata(timestamps []TimestampPair) map[string]string {
	metadata := make(map[string]string, len(timestamps))

	for i, ts := range timestamps {
		// Encode the index within the bundle as base-36
		key := big.NewInt(int64(i)).Text(36)

		// Calculate offset to save space (integration = queue + offset)
		offset := ts.IntegrateTimestampNanos - ts.QueueTimestampNanos

		// Encode both timestamps as base-36
		queueB36 := big.NewInt(ts.QueueTimestampNanos).Text(36)
		offsetB36 := big.NewInt(offset).Text(36)

		// Store as "queueTime offset"
		metadata[key] = fmt.Sprintf("%s %s", queueB36, offsetB36)
	}

	return metadata
}

// ParseTimestampMetadata extracts timestamp data from GCS object metadata.
// The entryIndexInBundle should be the index within the bundle (0-255).
func ParseTimestampMetadata(metadata map[string]string, entryIndexInBundle int64) (queueTS, integrateTS int64, err error) {
	// Convert index to base-36 key
	key := big.NewInt(entryIndexInBundle).Text(36)

	value, exists := metadata[key]
	if !exists {
		return 0, 0, fmt.Errorf("no timestamp metadata for entry index %d (key %q)", entryIndexInBundle, key)
	}

	// Parse "queueTime offset"
	queueTSB36, offsetB36, found := strings.Cut(value, " ")
	if !found {
		return 0, 0, fmt.Errorf("invalid timestamp format for key %q: %q (expected space-separated values)", key, value)
	}

	// Decode queue timestamp from base-36
	queueTime := new(big.Int)
	if _, ok := queueTime.SetString(queueTSB36, 36); !ok {
		return 0, 0, fmt.Errorf("invalid queue timestamp base-36 value: %q", queueTSB36)
	}

	// Decode offset from base-36
	offset := new(big.Int)
	if _, ok := offset.SetString(offsetB36, 36); !ok {
		return 0, 0, fmt.Errorf("invalid offset base-36 value: %q", offsetB36)
	}

	// Reconstruct integration timestamp
	integrateTime := queueTime.Int64() + offset.Int64()

	return queueTime.Int64(), integrateTime, nil
}
