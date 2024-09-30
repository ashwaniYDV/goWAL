# goWAL: a Write-Ahead Log in Go

`goWAL` is a high-performance `Write-Ahead Log (WAL)` library written in Go. 
It delivers fast read and write capabilities, making it an ideal choice for high-throughput applications.

## Key Features

- Append new entries to the log.
- Read all entries from the last (most recent) log segment.
- Read all entries from a given log segment offset.
- Automatic log rotation for faster recovery and startup.
- Automatic deletion of older segments when segment limit is reached.
- Periodic syncing of entries to disk from file buffer.
- CRC32 checksums for data integrity.
- Corrupted WALs are auto-repaired.
- Supports checkpointing for quick recovery.

### Concurrency Model

- **Read-write exclusivity:** The WAL is designed to ensure read / write exclusivity preventing concurrent reads and writes.
- **Thread safety:** The WAL is thread-safe, enabling concurrent writes from multiple threads without risk of data corruption.

### Log Segments

- **Immutable Segments:** Once created, log segments cannot be altered. Each entry within the segment is fixed and cannot be changed.
- **Sequential Numbering:** Segments are numbered starting from zero and increment sequentially.
- **Entry Sequencing:** Every entry in the log gets a sequence number, starting from 1, that spans across segments.

### Repair Functionality / Mechanism

- **Targeted Repair:** Only the last segment of the WAL is repaired if corruption is detected. This ensures minimal data loss.
- **Corruption Handling:** If a segment is corrupted, all segments following the corrupted one are discarded to prevent further data integrity issues.
- **Manual Deletion:** Corrupted segments beyond the first corrupted segment should be manually deleted before running the repair process.

## How to Use

### Creating a WAL

To initialize a new WAL, use the `OpenWAL` function by providing the directory path and other configuration options.

```go
wal, err := OpenWAL("/wal/directory", enableFsync, maxSegmentSize, maxSegments)
```

### Writing Entries

To append a new entry to the WAL, use the `WriteEntry` method. This method accepts a byte slice and ensures thread-safe operations.

```go
err := wal.WriteEntry([]byte("data"))
```

### Checkpointing the WAL

Checkpointing can be done with the `CreateCheckpoint` method, which flushes in-memory data and optionally allows storing metadata. 
This also syncs to disk if fsync is enabled.

```go
err := wal.CreateCheckpoint([]byte("checkpoint info"))
```

### Reading Entries from the WAL
- To read all entries from the most recent log segment, use `ReadAll`:

```go
// Read all entries from last segment
entries, err = wal.ReadAll(false)
if err != nil {
log.Fatalf("Failed to read entries: %v", err)
}

// Read all entries from last segment after the checkpoint
entries, err = wal.ReadAll(true)
if err != nil {
log.Fatalf("Failed to read entries: %v", err)
}
```

- To read from a specific log segment offset (inclusive), use `ReadAllFromOffset`:

```go
// Read all entries from a given offset
entries, err = wal.ReadAllFromOffset(offset, false)

// Read all entries from a given offset after the checkpoint
entries, err = wal.ReadAllFromOffset(offset, true)
```

### Restoring from the last available checkpoint

You can restore from the last checkpoint by reading all entries from the first available segment:

```go
entries, err = wal.ReadAllFromOffset(-1, true)
```

### Repairing the WAL (corrupted logs)

You can repair a corrupted WAL using the Repair method. This method returns the repaired entries, and atomically replaces the corrupted WAL file with the repaired one.

The WAL is capable of recovering from corrupted entries, as well as partial damage to the WAL file. However, if the file is completely corrupted, the WAL may not be able to recover from it and would proceed with replacing the file with an empty one.

```go
entries, err := wal.Repair()
```

### Closing the WAL

To close the WAL safely, use the `Close` method, which ensures all data is flushed and synced to disk before closure.

```go
err := wal.Close()
```

## Running Tests

The library includes test cases to validate its functionality. 
You can run the tests with the following command:

```bash
go test ./...
go test -bench=. -benchmem ./...
```