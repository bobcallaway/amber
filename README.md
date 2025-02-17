# amber
Presents a "multi-tenant" Trillian gRPC facade for "frozen" transparency logs that are stored in the C2SP tiles-tlog format in Google Cloud Storage

'insert cool amber logo here'

# Overview
This repo contains two programs:
- amber-polymerize
- amber-server

## amber-polymerize
This program reads entries from a given Trillian log and writes entries into a C2SP tlog-tiles compliant format into a GCS bucket

## amber-server
This program acts a multi-tenant facade of a Trillian Log Server, implementing the read-only subset of the log_server_rpc interface (as defined at https://github.com/google/trillian/blob/master/trillian_log_api.proto)
 - the program is configured to have a map of tree/log IDs to GCS buckets
 - for each incoming gRPC request, the program acts as a Tessera client to read entries from the GCS bucket that stores the log contents

## per-log GCS Bucket Layout
All paths are relative to the base of a GCS bucket:

```
/leafhash-to-index-map.db
/checkpoint
/tile/0/x001/x234/067
/tile/0/x001/x234/067.p/8
...
/tile/entries/000
/tile/entries/x000/111
/tile/entries/x000/x111/222
...
```

# Entry contents & metadata
When Trillian returns entries from a log, it responds with the following fields:
- MerkleTreeHash (the merkle leaf hash computed over LogLeaf)
- LogLeaf (the bytes for the entry itself)
- LogIndex (the index of the entry within the log)
- QueueTimestampNanos (the Unix time in Nanos for when the entry was queued for inclusion in the log)
- IntegrateTimestampNanos (the Unix time in Nanos for when the entry was integrated into the log)

The log index and leaf contents are known and the merkle leaf hash can be recomputed on the fly, but we need a way to return the previously stored timestamps.

The timestamps are stored in GCS object metadata which is a customer-defined key-value map. Recall though that objects will contain up to 256 entries in them, and 8 KiB is the metadata limit on an object in GCS.

Therefore we use object metadata as a `map[string]string` where a sample entry might be:

```
10: "1y2p0ij32e8e6 6355"
```

assuming keys are base-36 encoded indexes, and values are the queuetimestampnanos and integration offset separated with a space
(the actual integratetimenanos = queuetimestampnanos + offset)

keys would range from 0 to 255 (base-10) which is 0 - 73 (base-36); so the space required for all 256 keys is 36 + 2*220 = 476 bytes

since both timestamps are represented by int64 in Go, the longest base36 string for INT_MAX is 1y2p0ij32e8e7 (13 characters)
separating the two values with a space, this means each value will at maximum be 27 characters long

```
keyspace                   = 476 bytes  (utf-8 encoding)
256 values x 27 characters = 6912 bytes (utf-8 encoding)
---------------------------------------------------------
                             7388 bytes (7.22KiB) maximum
```

This allows us to persist the queue and integration timestamps in a non-disruptive way alongside the entries themselves.

# Questions to be answered
- Return time.Now() or a fixed value for Trillian STH timestamp_nanos (question around witness freshness?)
  - current CTFE https://ctfe.sigstore.dev/test/ct/v1/get-sth returns a fixed timestamp, but Rekor generates a new one?


# Deployment pattern - TBD
K8S Job amber-polymerize:
- acts as a trillian client, reading all values from a frozen log and:
    - copies values and metadata out into C2SP-compliant representation into a GCS bucket
    - builds and persists a BoltDB file for map[leafhash]index to support Get____ByHash APIs (stored as object inside same bucket)

K8S Deployment amber-server
- init container copies boltdb file from GCS into local volume for container; this optimizes reads from the local filesystem
- exposes read-only Trillian log service over gRPC that actually extracts values from C2SP tiles stored in GCS

K8S Service
- exposes amber-server endpoint to rest of cluster
