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

package gcs

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
)

// MetadataProvider is a function that returns metadata for a given object path.
// Return nil or empty map if no metadata should be added for the object.
type MetadataProvider func(objectPath string) map[string]string

// metadataTransport is an http.RoundTripper that injects custom metadata
// into GCS upload requests. It handles both multipart and resumable uploads.
type metadataTransport struct {
	base     http.RoundTripper
	provider MetadataProvider
}

// RoundTrip implements http.RoundTripper interface.
func (t *metadataTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Only handle uploads here; all other requests pass through unchanged
	if !strings.Contains(req.URL.Path, "/upload/") {
		return t.base.RoundTrip(req)
	}

	// Extract object name from URL query parameter
	objectName := req.URL.Query().Get("name")
	if objectName == "" {
		// Not a standard upload, pass through
		return t.base.RoundTrip(req)
	}

	// Get metadata for this object
	metadata := t.provider(objectName)
	if len(metadata) == 0 {
		// No metadata for this object
		return t.base.RoundTrip(req)
	}

	// Handle based on upload type
	uploadType := req.URL.Query().Get("uploadType")
	switch uploadType {
	case "multipart":
		return t.handleMultipartUpload(req, metadata)
	case "resumable", "":
		return t.handleResumableUpload(req, metadata)
	default:
		return t.base.RoundTrip(req)
	}
}

// handleResumableUpload injects metadata via x-goog-meta-* headers.
func (t *metadataTransport) handleResumableUpload(req *http.Request, metadata map[string]string) (*http.Response, error) {
	req = cloneRequest(req)
	for key, value := range metadata {
		req.Header.Set("x-goog-meta-"+key, value)
	}
	return t.base.RoundTrip(req)
}

// handleMultipartUpload injects metadata by modifying the JSON metadata in the multipart body.
func (t *metadataTransport) handleMultipartUpload(req *http.Request, metadata map[string]string) (*http.Response, error) {
	// Parse Content-Type to get boundary
	reqContentType := req.Header.Get("Content-Type")
	mediaType, params, err := mime.ParseMediaType(reqContentType)
	if err != nil || !strings.HasPrefix(mediaType, "multipart/") {
		return t.base.RoundTrip(req)
	}

	boundary := params["boundary"]
	if boundary == "" {
		return t.base.RoundTrip(req)
	}

	// Read original request body
	bodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		return t.base.RoundTrip(req)
	}
	_ = req.Body.Close() // Ignore error, we have the data

	// Parse multipart body
	mr := multipart.NewReader(bytes.NewReader(bodyBytes), boundary)
	var metadataPart []byte
	var contentPart []byte
	var contentType string

	for {
		p, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			return t.base.RoundTrip(req)
		}

		partData, err := io.ReadAll(p)
		if err != nil {
			req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			return t.base.RoundTrip(req)
		}

		if strings.Contains(p.Header.Get("Content-Type"), "application/json") {
			metadataPart = partData
		} else {
			contentPart = partData
			contentType = p.Header.Get("Content-Type")
		}
	}

	// Inject metadata into JSON
	if len(metadataPart) > 0 {
		var obj map[string]interface{}
		if err := json.Unmarshal(metadataPart, &obj); err != nil {
			req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			return t.base.RoundTrip(req)
		}

		// Add or update metadata field
		if obj["metadata"] == nil {
			obj["metadata"] = make(map[string]string)
		}

		metadataMap, ok := obj["metadata"].(map[string]interface{})
		if !ok {
			metadataMap = make(map[string]interface{})
			obj["metadata"] = metadataMap
		}

		for key, value := range metadata {
			metadataMap[key] = value
		}

		metadataPart, err = json.Marshal(obj)
		if err != nil {
			req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			return t.base.RoundTrip(req)
		}
	}

	// Reconstruct multipart body
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	if err := mw.SetBoundary(boundary); err != nil {
		// If we can't set boundary, fall back to original request
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		return t.base.RoundTrip(req)
	}

	if len(metadataPart) > 0 {
		partWriter, err := mw.CreatePart(map[string][]string{
			"Content-Type": {"application/json; charset=UTF-8"},
		})
		if err != nil {
			req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			return t.base.RoundTrip(req)
		}
		if _, err := partWriter.Write(metadataPart); err != nil {
			req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			return t.base.RoundTrip(req)
		}
	}

	if len(contentPart) > 0 {
		partWriter, err := mw.CreatePart(map[string][]string{
			"Content-Type": {contentType},
		})
		if err != nil {
			req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			return t.base.RoundTrip(req)
		}
		if _, err := partWriter.Write(contentPart); err != nil {
			req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			return t.base.RoundTrip(req)
		}
	}

	if err := mw.Close(); err != nil {
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		return t.base.RoundTrip(req)
	}

	req = cloneRequest(req)
	req.Body = io.NopCloser(&buf)
	req.ContentLength = int64(buf.Len())

	return t.base.RoundTrip(req)
}

// cloneRequest creates a shallow copy of the request with a cloned header map.
func cloneRequest(req *http.Request) *http.Request {
	r := new(http.Request)
	*r = *req
	r.Header = make(http.Header, len(req.Header))
	for k, v := range req.Header {
		r.Header[k] = v
	}
	return r
}

// NewClientWithMetadata creates a GCS client that automatically injects metadata
// into upload operations using the provided MetadataProvider function.
//
// The MetadataProvider is called for each upload with the object path, allowing
// dynamic metadata generation based on what's being uploaded.
//
// Example with static metadata for all objects:
//
//	provider := func(objectPath string) map[string]string {
//	    return map[string]string{
//	        "source": "my-application",
//	        "version": "1.0",
//	    }
//	}
//	client, err := gcs.NewClientWithMetadata(ctx, provider)
//
// Example with dynamic metadata based on path:
//
//	provider := func(objectPath string) map[string]string {
//	    if strings.HasPrefix(objectPath, "tile/entries/") {
//	        return map[string]string{"type": "entry"}
//	    }
//	    return nil  // No metadata for other objects
//	}
//	client, err := gcs.NewClientWithMetadata(ctx, provider)
func NewClientWithMetadata(ctx context.Context, provider MetadataProvider, opts ...option.ClientOption) (*storage.Client, error) {
	// If using emulator, skip authentication and use default emulator handling in client
	if os.Getenv("STORAGE_EMULATOR_HOST") != "" {
		customHTTPClient := &http.Client{
			Transport: &metadataTransport{
				base:     http.DefaultTransport,
				provider: provider,
			},
		}
		allOpts := append([]option.ClientOption{
			option.WithHTTPClient(customHTTPClient),
			option.WithoutAuthentication(),
		}, opts...)
		return storage.NewClient(ctx, allOpts...)
	}

	// Using production GCS with authentication
	// Get default credentials for real GCS
	creds, err := google.FindDefaultCredentials(ctx, storage.ScopeFullControl)
	if err != nil {
		return nil, err
	} // Create authenticated transport
	authTransport, err := htransport.NewTransport(ctx, http.DefaultTransport, option.WithCredentials(creds))
	if err != nil {
		return nil, err
	}

	// Wrap with our metadata-injecting transport
	customHTTPClient := &http.Client{
		Transport: &metadataTransport{
			base:     authTransport,
			provider: provider,
		},
	}

	// Create storage client
	allOpts := append([]option.ClientOption{option.WithHTTPClient(customHTTPClient)}, opts...)
	return storage.NewClient(ctx, allOpts...)
}
