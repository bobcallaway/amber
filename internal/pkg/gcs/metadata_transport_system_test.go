//go:build integration
// +build integration

package gcs

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// TestMetadataRoundTripperSystemTest creates actual GCS objects and verifies
// that custom metadata is properly set on them.
//
// This test requires:
// - A GCS bucket to exist (set via GCS_TEST_BUCKET env var)
// - Appropriate GCS credentials (via default application credentials)
//
// Run with: go test -tags=integration -v ./internal/pkg/gcs/
func TestMetadataRoundTripperSystemTest(t *testing.T) {
	bucket := os.Getenv("GCS_TEST_BUCKET")
	if bucket == "" {
		t.Skip("GCS_TEST_BUCKET not set, skipping system test")
	}

	ctx := context.Background()

	// Define test metadata
	testMetadata := map[string]string{
		"source":      "amber-system-test",
		"timestamp":   fmt.Sprintf("%d", time.Now().Unix()),
		"test-key":    "test-value",
		"environment": "testing",
	}

	// Create a client with metadata injection
	// Provider that returns static metadata for all objects
	provider := func(objectPath string) map[string]string {
		return testMetadata
	}
	client, err := NewClientWithMetadata(ctx, provider)
	if err != nil {
		t.Fatalf("failed to create GCS client: %v", err)
	}
	defer client.Close()

	// Create a unique object name for this test run
	objectName := fmt.Sprintf("test-metadata-%d.txt", time.Now().UnixNano())

	t.Logf("Creating test object: gs://%s/%s", bucket, objectName)

	// Write an object to GCS
	obj := client.Bucket(bucket).Object(objectName)
	w := obj.NewWriter(ctx)
	testContent := []byte("This is a test object for metadata verification")
	if _, err := w.Write(testContent); err != nil {
		t.Fatalf("failed to write object data: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close object writer: %v", err)
	}

	// Clean up the test object when done
	defer func() {
		if err := obj.Delete(ctx); err != nil {
			t.Errorf("failed to delete test object: %v", err)
		}
	}()

	// Read back the object attributes to verify metadata
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		t.Fatalf("failed to get object attributes: %v", err)
	}

	t.Logf("Object created successfully with size: %d bytes", attrs.Size)

	// Verify all expected metadata keys are present and correct
	if attrs.Metadata == nil {
		t.Fatal("object metadata is nil")
	}

	for key, expectedValue := range testMetadata {
		actualValue, exists := attrs.Metadata[key]
		if !exists {
			t.Errorf("metadata key %q not found in object", key)
			continue
		}
		if actualValue != expectedValue {
			t.Errorf("metadata key %q: got %q, want %q", key, actualValue, expectedValue)
		}
	}

	// Log all metadata for debugging
	t.Logf("Object metadata contains %d keys:", len(attrs.Metadata))
	for k, v := range attrs.Metadata {
		t.Logf("  %s: %s", k, v)
	}

	// Verify no unexpected metadata keys (this is optional but good practice)
	if len(attrs.Metadata) != len(testMetadata) {
		t.Logf("Warning: object has %d metadata keys, expected %d", len(attrs.Metadata), len(testMetadata))
	}
}

// TestMultipleObjectsMetadata tests that metadata is correctly applied to
// multiple objects written through the same client.
func TestMultipleObjectsMetadata(t *testing.T) {
	bucket := os.Getenv("GCS_TEST_BUCKET")
	if bucket == "" {
		t.Skip("GCS_TEST_BUCKET not set, skipping system test")
	}

	ctx := context.Background()

	testMetadata := map[string]string{
		"source": "amber-multi-test",
		"batch":  fmt.Sprintf("batch-%d", time.Now().Unix()),
	}

	provider := func(objectPath string) map[string]string {
		return testMetadata
	}
	client, err := NewClientWithMetadata(ctx, provider)
	if err != nil {
		t.Fatalf("failed to create GCS client: %v", err)
	}
	defer client.Close()

	// Create multiple objects
	numObjects := 3
	objectNames := make([]string, numObjects)

	for i := 0; i < numObjects; i++ {
		objectNames[i] = fmt.Sprintf("test-metadata-multi-%d-%d.txt", time.Now().UnixNano(), i)

		obj := client.Bucket(bucket).Object(objectNames[i])
		w := obj.NewWriter(ctx)
		content := []byte(fmt.Sprintf("Test object %d", i))

		if _, err := w.Write(content); err != nil {
			t.Fatalf("failed to write object %d: %v", i, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("failed to close object writer %d: %v", i, err)
		}

		// Clean up
		defer func(name string) {
			if err := client.Bucket(bucket).Object(name).Delete(ctx); err != nil {
				t.Errorf("failed to delete test object %s: %v", name, err)
			}
		}(objectNames[i])

		// Small delay between writes
		time.Sleep(100 * time.Millisecond)
	}

	// Verify metadata on all objects
	for i, objectName := range objectNames {
		t.Logf("Verifying object %d: %s", i, objectName)

		attrs, err := client.Bucket(bucket).Object(objectName).Attrs(ctx)
		if err != nil {
			t.Fatalf("failed to get attributes for object %d: %v", i, err)
		}

		if attrs.Metadata == nil {
			t.Fatalf("object %d: metadata is nil", i)
		}

		for key, expectedValue := range testMetadata {
			actualValue, exists := attrs.Metadata[key]
			if !exists {
				t.Errorf("object %d: metadata key %q not found", i, key)
				continue
			}
			if actualValue != expectedValue {
				t.Errorf("object %d: metadata key %q: got %q, want %q", i, key, actualValue, expectedValue)
			}
		}
	}
}

// TestMetadataWithoutCustomTransport verifies that objects created without
// the custom transport don't have the custom metadata (negative test).
func TestMetadataWithoutCustomTransport(t *testing.T) {
	bucket := os.Getenv("GCS_TEST_BUCKET")
	if bucket == "" {
		t.Skip("GCS_TEST_BUCKET not set, skipping system test")
	}

	ctx := context.Background()

	// Create a standard client WITHOUT metadata injection
	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatalf("failed to create GCS client: %v", err)
	}
	defer client.Close()

	objectName := fmt.Sprintf("test-no-metadata-%d.txt", time.Now().UnixNano())

	obj := client.Bucket(bucket).Object(objectName)
	w := obj.NewWriter(ctx)
	if _, err := w.Write([]byte("Object without custom metadata")); err != nil {
		t.Fatalf("failed to write object: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	defer func() {
		if err := obj.Delete(ctx); err != nil {
			t.Errorf("failed to delete test object: %v", err)
		}
	}()

	attrs, err := obj.Attrs(ctx)
	if err != nil {
		t.Fatalf("failed to get object attributes: %v", err)
	}

	// Verify that our custom metadata keys are NOT present
	if attrs.Metadata != nil {
		if _, exists := attrs.Metadata["source"]; exists {
			t.Error("unexpected 'source' metadata found on object created without custom transport")
		}
	}

	t.Logf("Verified that object created without custom transport has no custom metadata")
}

// TestMetadataLargeObject tests metadata handling with a larger object
// to ensure the transport works with chunked uploads.
func TestMetadataLargeObject(t *testing.T) {
	bucket := os.Getenv("GCS_TEST_BUCKET")
	if bucket == "" {
		t.Skip("GCS_TEST_BUCKET not set, skipping system test")
	}

	ctx := context.Background()

	testMetadata := map[string]string{
		"source":      "amber-large-test",
		"object-type": "large",
		"test-case":   "chunked-upload",
	}

	provider := func(objectPath string) map[string]string {
		return testMetadata
	}
	client, err := NewClientWithMetadata(ctx, provider)
	if err != nil {
		t.Fatalf("failed to create GCS client: %v", err)
	}
	defer client.Close()

	objectName := fmt.Sprintf("test-metadata-large-%d.bin", time.Now().UnixNano())

	obj := client.Bucket(bucket).Object(objectName)
	w := obj.NewWriter(ctx)

	// Write 5MB of data to trigger resumable upload
	const chunkSize = 1024 * 1024 // 1MB
	const numChunks = 5
	data := make([]byte, chunkSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	for i := 0; i < numChunks; i++ {
		if _, err := w.Write(data); err != nil {
			t.Fatalf("failed to write chunk %d: %v", i, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	defer func() {
		if err := obj.Delete(ctx); err != nil {
			t.Errorf("failed to delete test object: %v", err)
		}
	}()

	// Verify metadata
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		t.Fatalf("failed to get object attributes: %v", err)
	}

	t.Logf("Large object created successfully with size: %d bytes", attrs.Size)

	if attrs.Metadata == nil {
		t.Fatal("object metadata is nil")
	}

	for key, expectedValue := range testMetadata {
		actualValue, exists := attrs.Metadata[key]
		if !exists {
			t.Errorf("metadata key %q not found in large object", key)
			continue
		}
		if actualValue != expectedValue {
			t.Errorf("metadata key %q: got %q, want %q", key, actualValue, expectedValue)
		}
	}
}

// TestMetadataSpecialCharacters tests that metadata values with special
// characters are properly handled.
func TestMetadataSpecialCharacters(t *testing.T) {
	bucket := os.Getenv("GCS_TEST_BUCKET")
	if bucket == "" {
		t.Skip("GCS_TEST_BUCKET not set, skipping system test")
	}

	ctx := context.Background()

	// Test various special characters that should be valid in metadata
	testMetadata := map[string]string{
		"source":      "amber-special-chars",
		"path":        "/path/to/resource",
		"url":         "https://example.com/resource?param=value&other=123",
		"description": "Test with spaces and punctuation!",
		"unicode":     "Hello 世界 🌍",
	}

	provider := func(objectPath string) map[string]string {
		return testMetadata
	}
	client, err := NewClientWithMetadata(ctx, provider)
	if err != nil {
		t.Fatalf("failed to create GCS client: %v", err)
	}
	defer client.Close()

	objectName := fmt.Sprintf("test-metadata-special-%d.txt", time.Now().UnixNano())

	obj := client.Bucket(bucket).Object(objectName)
	w := obj.NewWriter(ctx)
	if _, err := w.Write([]byte("Object with special character metadata")); err != nil {
		t.Fatalf("failed to write object: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	defer func() {
		if err := obj.Delete(ctx); err != nil {
			t.Errorf("failed to delete test object: %v", err)
		}
	}()

	attrs, err := obj.Attrs(ctx)
	if err != nil {
		t.Fatalf("failed to get object attributes: %v", err)
	}

	if attrs.Metadata == nil {
		t.Fatal("object metadata is nil")
	}

	for key, expectedValue := range testMetadata {
		actualValue, exists := attrs.Metadata[key]
		if !exists {
			t.Errorf("metadata key %q not found", key)
			continue
		}
		if actualValue != expectedValue {
			t.Errorf("metadata key %q: got %q, want %q", key, actualValue, expectedValue)
		}
	}

	t.Log("All special character metadata verified successfully")
}

// TestListObjectsWithMetadata verifies that objects with custom metadata
// can be properly listed and their metadata accessed.
func TestListObjectsWithMetadata(t *testing.T) {
	bucket := os.Getenv("GCS_TEST_BUCKET")
	if bucket == "" {
		t.Skip("GCS_TEST_BUCKET not set, skipping system test")
	}

	ctx := context.Background()

	prefix := fmt.Sprintf("test-metadata-list-%d-", time.Now().UnixNano())
	testMetadata := map[string]string{
		"source": "amber-list-test",
		"prefix": prefix,
	}

	provider := func(objectPath string) map[string]string {
		return testMetadata
	}
	client, err := NewClientWithMetadata(ctx, provider)
	if err != nil {
		t.Fatalf("failed to create GCS client: %v", err)
	}
	defer client.Close()

	// Create a few objects with the same prefix
	numObjects := 3
	for i := 0; i < numObjects; i++ {
		objectName := fmt.Sprintf("%sobject-%d.txt", prefix, i)
		obj := client.Bucket(bucket).Object(objectName)
		w := obj.NewWriter(ctx)
		if _, err := w.Write([]byte(fmt.Sprintf("Object %d", i))); err != nil {
			t.Fatalf("failed to write object %d: %v", i, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("failed to close writer %d: %v", i, err)
		}

		time.Sleep(100 * time.Millisecond)
	}

	// Clean up all objects
	defer func() {
		it := client.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: prefix})
		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Errorf("error iterating objects: %v", err)
				break
			}
			if err := client.Bucket(bucket).Object(attrs.Name).Delete(ctx); err != nil {
				t.Errorf("failed to delete object %s: %v", attrs.Name, err)
			}
		}
	}()

	// List objects and verify metadata
	query := &storage.Query{Prefix: prefix}
	it := client.Bucket(bucket).Objects(ctx, query)

	foundObjects := 0
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("error iterating objects: %v", err)
		}

		foundObjects++
		t.Logf("Found object: %s", attrs.Name)

		// Note: The iterator returns ObjectAttrs, but Metadata field may not be
		// populated by default. We need to fetch full attributes.
		fullAttrs, err := client.Bucket(bucket).Object(attrs.Name).Attrs(ctx)
		if err != nil {
			t.Fatalf("failed to get full attributes for %s: %v", attrs.Name, err)
		}

		if fullAttrs.Metadata == nil {
			t.Fatalf("object %s has nil metadata", attrs.Name)
		}

		for key, expectedValue := range testMetadata {
			actualValue, exists := fullAttrs.Metadata[key]
			if !exists {
				t.Errorf("object %s: metadata key %q not found", attrs.Name, key)
				continue
			}
			if actualValue != expectedValue {
				t.Errorf("object %s: metadata key %q: got %q, want %q", attrs.Name, key, actualValue, expectedValue)
			}
		}
	}

	if foundObjects != numObjects {
		t.Errorf("found %d objects, expected %d", foundObjects, numObjects)
	}
}
