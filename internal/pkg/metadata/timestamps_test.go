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
	"math"
	"testing"
)

func TestBuildTimestampMetadata(t *testing.T) {
	tests := []struct {
		name       string
		timestamps []TimestampPair
		wantKeys   []string
	}{
		{
			name: "single entry",
			timestamps: []TimestampPair{
				{QueueTimestampNanos: 1000000000, IntegrateTimestampNanos: 1000001000},
			},
			wantKeys: []string{"0"},
		},
		{
			name: "multiple entries",
			timestamps: []TimestampPair{
				{QueueTimestampNanos: 1000000000, IntegrateTimestampNanos: 1000001000},
				{QueueTimestampNanos: 2000000000, IntegrateTimestampNanos: 2000002000},
				{QueueTimestampNanos: 3000000000, IntegrateTimestampNanos: 3000003000},
			},
			wantKeys: []string{"0", "1", "2"},
		},
		{
			name: "max int64 values",
			timestamps: []TimestampPair{
				{QueueTimestampNanos: math.MaxInt64, IntegrateTimestampNanos: math.MaxInt64},
			},
			wantKeys: []string{"0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata := BuildTimestampMetadata(tt.timestamps)

			// Check that all expected keys are present
			if len(metadata) != len(tt.wantKeys) {
				t.Errorf("BuildTimestampMetadata() returned %d keys, want %d", len(metadata), len(tt.wantKeys))
			}

			for _, key := range tt.wantKeys {
				if _, ok := metadata[key]; !ok {
					t.Errorf("BuildTimestampMetadata() missing key %q", key)
				}
			}
		})
	}
}

func TestParseTimestampMetadata(t *testing.T) {
	tests := []struct {
		name                string
		metadata            map[string]string
		entryIndexInBundle  int64
		wantQueue           int64
		wantIntegrate       int64
		wantErr             bool
	}{
		{
			name: "valid entry at index 0",
			metadata: map[string]string{
				"0": "rs rs", // 1000 and offset 1000 in base-36
			},
			entryIndexInBundle: 0,
			wantQueue:          1000,
			wantIntegrate:      2000,
			wantErr:            false,
		},
		{
			name: "valid entry at index 10",
			metadata: map[string]string{
				"a": "rs rs", // index 10 in base-36 is 'a'
			},
			entryIndexInBundle: 10,
			wantQueue:          1000,
			wantIntegrate:      2000,
			wantErr:            false,
		},
		{
			name: "missing entry",
			metadata: map[string]string{
				"0": "rs 3e8",
			},
			entryIndexInBundle: 1,
			wantErr:            true,
		},
		{
			name: "invalid format - no space",
			metadata: map[string]string{
				"0": "rs3e8",
			},
			entryIndexInBundle: 0,
			wantErr:            true,
		},
		{
			name: "invalid format - invalid base-36",
			metadata: map[string]string{
				"0": "not@valid xyz!", // @ and ! are not valid base-36 characters
			},
			entryIndexInBundle: 0,
			wantErr:            true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotQueue, gotIntegrate, err := ParseTimestampMetadata(tt.metadata, tt.entryIndexInBundle)

			if (err != nil) != tt.wantErr {
				t.Errorf("ParseTimestampMetadata() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err == nil {
				if gotQueue != tt.wantQueue {
					t.Errorf("ParseTimestampMetadata() queue = %v, want %v", gotQueue, tt.wantQueue)
				}
				if gotIntegrate != tt.wantIntegrate {
					t.Errorf("ParseTimestampMetadata() integrate = %v, want %v", gotIntegrate, tt.wantIntegrate)
				}
			}
		})
	}
}

func TestRoundTrip(t *testing.T) {
	// Test that we can encode and decode timestamps correctly
	timestamps := []TimestampPair{
		{QueueTimestampNanos: 1609459200000000000, IntegrateTimestampNanos: 1609459201000000000}, // 2021-01-01
		{QueueTimestampNanos: 1640995200000000000, IntegrateTimestampNanos: 1640995300000000000}, // 2022-01-01
		{QueueTimestampNanos: 1672531200000000000, IntegrateTimestampNanos: 1672531500000000000}, // 2023-01-01
	}

	// Build metadata
	metadata := BuildTimestampMetadata(timestamps)

	// Parse each entry back
	for i, expected := range timestamps {
		gotQueue, gotIntegrate, err := ParseTimestampMetadata(metadata, int64(i))
		if err != nil {
			t.Fatalf("ParseTimestampMetadata(%d) error = %v", i, err)
		}

		if gotQueue != expected.QueueTimestampNanos {
			t.Errorf("Entry %d: queue = %v, want %v", i, gotQueue, expected.QueueTimestampNanos)
		}

		if gotIntegrate != expected.IntegrateTimestampNanos {
			t.Errorf("Entry %d: integrate = %v, want %v", i, gotIntegrate, expected.IntegrateTimestampNanos)
		}
	}
}

func TestMetadataSize(t *testing.T) {
	// Test that 256 entries fit within the 8 KiB limit
	timestamps := make([]TimestampPair, 256)
	for i := 0; i < 256; i++ {
		// Use max int64 values to test worst case
		timestamps[i] = TimestampPair{
			QueueTimestampNanos:     math.MaxInt64,
			IntegrateTimestampNanos: math.MaxInt64,
		}
	}

	metadata := BuildTimestampMetadata(timestamps)

	// Calculate total size
	totalSize := 0
	for k, v := range metadata {
		totalSize += len(k) + len(v)
	}

	// Should be less than 8 KiB (8192 bytes)
	maxSize := 8192
	if totalSize > maxSize {
		t.Errorf("Metadata size %d bytes exceeds limit of %d bytes", totalSize, maxSize)
	}

	t.Logf("Metadata size for 256 max-value entries: %d bytes (%.2f KiB)", totalSize, float64(totalSize)/1024)
}
