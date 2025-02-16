package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

const (
	syncInterval  = 200 * time.Millisecond
	segmentPrefix = "segment-"
)

// WAL structure
type WAL struct {
	directory           string
	currentSegment      *os.File
	currentSegmentIndex int
	lastSequenceNo      uint64
	shouldFsync         bool
	maxFileSize         int64
	maxSegments         int
	bufWriter           *bufio.Writer
	syncTimer           *time.Timer
	lock                sync.Mutex
	ctx                 context.Context
	cancel              context.CancelFunc
}

// OpenWAL initialize a new WAL.
// If the directory does not exist, it will be created.
// If the directory exists, the last log segment file will be opened and the last sequence number will be read from it.
// enableFsync enables fsync on the log segment file every time the log flushes.
// maxFileSize is the maximum size of a log segment file in bytes.
// maxSegments is the maximum number of log segment files to keep.
func OpenWAL(directory string, enableFsync bool, maxFileSize int64, maxSegments int) (*WAL, error) {
	// Create the directory if it doesn't exist
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	// Get the list of log segment files in the directory
	files, err := filepath.Glob(filepath.Join(directory, segmentPrefix+"*"))
	if err != nil {
		return nil, err
	}

	var lastSegmentID int
	if len(files) > 0 {
		// Find the last segment ID
		lastSegmentID, err = findLastSegmentIndexInFiles(files)
		if err != nil {
			return nil, err
		}
	} else {
		// Create the first log segment
		file, err := createSegmentFile(directory, 0)
		if err != nil {
			return nil, err
		}

		if err := file.Close(); err != nil {
			return nil, err
		}
	}

	// Open the last log segment file
	filePath := filepath.Join(directory, fmt.Sprintf("%s%d", segmentPrefix, lastSegmentID))
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	// Seek to the end of the file
	if _, err = file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	// ctx and cancel are used to control the go routines
	ctx, cancel := context.WithCancel(context.Background())

	wal := &WAL{
		directory:           directory,
		currentSegment:      file,
		lastSequenceNo:      0,
		bufWriter:           bufio.NewWriter(file),
		syncTimer:           time.NewTimer(syncInterval), // syncInterval is a predefined duration
		shouldFsync:         enableFsync,
		maxFileSize:         maxFileSize,
		maxSegments:         maxSegments,
		currentSegmentIndex: lastSegmentID,
		ctx:                 ctx,
		cancel:              cancel,
	}

	if wal.lastSequenceNo, err = wal.getLastSequenceNo(); err != nil {
		return nil, err
	}

	// fire a separate go routine for syncing the current log segment file
	go wal.keepSyncing()

	return wal, nil
}

// WriteEntry writes an entry to the WAL.
func (wal *WAL) WriteEntry(data []byte) error {
	return wal.writeEntry(data, false)
}

// CreateCheckpoint creates a checkpoint entry in the WAL.
// A checkpoint entry is a special entry that can be used to restore the state of the system to the point when the checkpoint was created.
func (wal *WAL) CreateCheckpoint(data []byte) error {
	return wal.writeEntry(data, true)
}

func (wal *WAL) writeEntry(data []byte, isCheckpoint bool) error {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	if err := wal.rotateLogIfNeeded(); err != nil {
		return err
	}

	wal.lastSequenceNo++
	entry := &WAL_Entry{
		LogSequenceNumber: wal.lastSequenceNo,
		Data:              data,
		CRC:               crc32.ChecksumIEEE(append(data, byte(wal.lastSequenceNo))),
	}

	if isCheckpoint {
		if err := wal.Sync(); err != nil {
			return fmt.Errorf("could not create checkpoint, error while syncing: %v", err)
		}
		entry.IsCheckpoint = &isCheckpoint
	}

	// initially writing the entry to in-memory buffer for faster writes
	// periodic syncing to disc is done by the separate go-routine
	return wal.writeEntryToBuffer(entry)
}

func (wal *WAL) writeEntryToBuffer(entry *WAL_Entry) error {
	marshaledEntry := MustMarshal(entry)

	size := int32(len(marshaledEntry))
	if err := binary.Write(wal.bufWriter, binary.LittleEndian, size); err != nil {
		return err
	}
	_, err := wal.bufWriter.Write(marshaledEntry)

	return err
}

func (wal *WAL) rotateLogIfNeeded() error {
	fileInfo, err := wal.currentSegment.Stat()
	if err != nil {
		return err
	}

	if fileInfo.Size()+int64(wal.bufWriter.Buffered()) >= wal.maxFileSize {
		if err := wal.rotateLog(); err != nil {
			return err
		}
	}

	return nil
}

func (wal *WAL) rotateLog() error {
	if err := wal.Sync(); err != nil {
		return err
	}

	if err := wal.currentSegment.Close(); err != nil {
		return err
	}

	wal.currentSegmentIndex++
	if wal.currentSegmentIndex >= wal.maxSegments {
		if err := wal.deleteOldestSegment(); err != nil {
			return err
		}
	}

	newFile, err := createSegmentFile(wal.directory, wal.currentSegmentIndex)
	if err != nil {
		return err
	}

	wal.currentSegment = newFile
	wal.bufWriter = bufio.NewWriter(newFile)

	return nil
}

// removes the oldest log file
func (wal *WAL) deleteOldestSegment() error {
	files, err := filepath.Glob(filepath.Join(wal.directory, segmentPrefix+"*"))
	if err != nil {
		return err
	}

	var oldestSegmentFilePath string
	if len(files) > 0 {
		// Find the oldest segment ID
		oldestSegmentFilePath, err = wal.findOldestSegmentFile(files)
		if err != nil {
			return err
		}
	} else {
		return nil
	}

	// Delete the oldest segment file
	if err := os.Remove(oldestSegmentFilePath); err != nil {
		return err
	}

	return nil
}

func (wal *WAL) findOldestSegmentFile(files []string) (string, error) {
	var oldestSegmentFilePath string
	oldestSegmentID := math.MaxInt64
	for _, file := range files {
		// Get the segment index from the file name
		segmentIndex, err := strconv.Atoi(strings.TrimPrefix(file, filepath.Join(wal.directory, segmentPrefix)))
		if err != nil {
			return "", err
		}

		if segmentIndex < oldestSegmentID {
			oldestSegmentID = segmentIndex
			oldestSegmentFilePath = file
		}
	}

	return oldestSegmentFilePath, nil
}

// Close the WAL file. It also calls Sync() on the WAL.
func (wal *WAL) Close() error {
	wal.cancel()
	if err := wal.Sync(); err != nil {
		return err
	}
	return wal.currentSegment.Close()
}

// ReadAll reads all entries from the WAL.
// If readFromCheckpoint is true, it will return all the entries from the last checkpoint
// (if no checkpoint is found, it will return an empty slice.)
func (wal *WAL) ReadAll(readFromCheckpoint bool) ([]*WAL_Entry, error) {
	file, err := os.OpenFile(wal.currentSegment.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	entries, checkpoint, err := readAllEntriesFromFile(file, readFromCheckpoint)
	if err != nil {
		return entries, err
	}

	if readFromCheckpoint && checkpoint <= 0 {
		// Empty the entries slice
		return entries[:0], nil
	}

	return entries, nil
}

// ReadAllFromOffset starts reading from log segment files starting from the given offset (Segment Index) and returns all the entries.
// If readFromCheckpoint is true, it will return all the entries from the last checkpoint
// (if no checkpoint is found, it will return an empty slice.)
//
// entries, err = wal.ReadAllFromOffset(-1, true)
// this will start scanning from the first available segment, and get all entries after the last checkpoint
// Note: segment offset starts from 0
func (wal *WAL) ReadAllFromOffset(offset int, readFromCheckpoint bool) ([]*WAL_Entry, error) {
	// Get the list of log segment files in the directory
	files, err := filepath.Glob(filepath.Join(wal.directory, segmentPrefix+"*"))
	if err != nil {
		return nil, err
	}

	var entries []*WAL_Entry
	prevCheckpointLogSequenceNo := uint64(0)

	for _, file := range files {
		// Get the segment index from the file name
		segmentIndex, err := strconv.Atoi(strings.TrimPrefix(file, filepath.Join(wal.directory, "segment-")))
		if err != nil {
			return entries, err
		}

		if segmentIndex < offset {
			continue
		}

		file, err := os.OpenFile(file, os.O_RDONLY, 0644)
		if err != nil {
			return nil, err
		}

		entriesFromSegment, checkpoint, err := readAllEntriesFromFile(file, readFromCheckpoint)
		if err != nil {
			return entries, err
		}

		// If we find the latest checkpoint,
		// we should return the entries from this latest checkpoint
		// So we empty the entries slice and start appending entries from this checkpoint.
		if readFromCheckpoint && checkpoint > prevCheckpointLogSequenceNo {
			entries = entries[:0]
			prevCheckpointLogSequenceNo = checkpoint
		}

		entries = append(entries, entriesFromSegment...)
	}

	return entries, nil
}

func readAllEntriesFromFile(file *os.File, readFromCheckpoint bool) ([]*WAL_Entry, uint64, error) {
	var entries []*WAL_Entry
	checkpointLogSequenceNo := uint64(0)
	for {
		var size int32
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				break
			}
			return entries, checkpointLogSequenceNo, err
		}

		data := make([]byte, size)
		if _, err := io.ReadFull(file, data); err != nil {
			return entries, checkpointLogSequenceNo, err
		}

		entry, err := unmarshalAndVerifyEntry(data)
		if err != nil {
			return entries, checkpointLogSequenceNo, err
		}

		// If we are reading from checkpoint, and we find a checkpoint entry,
		// we should return the entries from the last checkpoint.
		// So we empty the entries slice and start appending entries from the checkpoint.
		if readFromCheckpoint && entry.IsCheckpoint != nil && entry.GetIsCheckpoint() {
			checkpointLogSequenceNo = entry.GetLogSequenceNumber()
			// Empty the entries slice
			entries = entries[:0]
		}

		entries = append(entries, entry)
	}

	return entries, checkpointLogSequenceNo, nil
}

// Sync writes out any data in the WAL's in-memory buffer to the segment file.
// If fsync is enabled, it also calls fsync on the segment file.
// It also resets the synchronization timer.
func (wal *WAL) Sync() error {
	if err := wal.bufWriter.Flush(); err != nil {
		return err
	}
	if wal.shouldFsync {
		if err := wal.currentSegment.Sync(); err != nil {
			return err
		}
	}

	// Reset the keepSyncing timer, since we just synced.
	wal.resetTimer()

	return nil
}

// resetTimer resets the synchronization timer.
func (wal *WAL) resetTimer() {
	wal.syncTimer.Reset(syncInterval)
}

func (wal *WAL) keepSyncing() {
	for {
		select {
		case <-wal.syncTimer.C:

			wal.lock.Lock()
			err := wal.Sync()
			wal.lock.Unlock()

			if err != nil {
				log.Printf("Error while performing sync: %v", err)
			}

		case <-wal.ctx.Done():
			return
		}
	}
}

// Repair repairs a corrupted WAL by scanning the WAL from the start and
// reading all entries until a corrupted entry is encountered, at which point the file is truncated.
// The function returns the entries that were read before the corruption and overwrites the existing WAL file with the repaired entries.
// It checks the CRC of each entry to verify if it is corrupted, and if the CRC is invalid,
// the file is truncated at that point.
func (wal *WAL) Repair() ([]*WAL_Entry, error) {
	files, err := filepath.Glob(filepath.Join(wal.directory, segmentPrefix+"*"))
	if err != nil {
		return nil, err
	}

	var lastSegmentID int
	if len(files) > 0 {
		// Find the last segment ID
		lastSegmentID, err = findLastSegmentIndexInFiles(files)
		if err != nil {
			return nil, err
		}
	} else {
		log.Fatalf("No log segments found, nothing to repair.")
	}
	// Open the last log segment file
	filePath := filepath.Join(wal.directory, fmt.Sprintf("%s%d", segmentPrefix, lastSegmentID))
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	// Seek to the beginning of the file
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var entries []*WAL_Entry

	for {
		// Read the size of the next entry.
		var size int32
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				// End of file reached, no corruption found.
				return entries, err
			}
			log.Printf("Error while reading entry size: %v", err)
			// Truncate the file at this point.
			if err := wal.replaceWithFixedFile(entries); err != nil {
				return entries, err
			}
			return nil, nil
		}

		// Read the entry data.
		data := make([]byte, size)
		if _, err := io.ReadFull(file, data); err != nil {
			// Truncate the file at this point
			if err := wal.replaceWithFixedFile(entries); err != nil {
				return entries, err
			}
			return entries, nil
		}

		// Deserialize the entry.
		var entry WAL_Entry
		if err := proto.Unmarshal(data, &entry); err != nil {
			if err := wal.replaceWithFixedFile(entries); err != nil {
				return entries, err
			}
			return entries, nil
		}

		if !verifyCRC(&entry) {
			log.Printf("CRC mismatch: data may be corrupted")
			// Truncate the file at this point
			if err := wal.replaceWithFixedFile(entries); err != nil {
				return entries, err
			}

			return entries, nil
		}

		// Add the entry to the slice.
		entries = append(entries, &entry)
	}
}

// replaceWithFixedFile replaces the existing WAL file with the given entries atomically.
func (wal *WAL) replaceWithFixedFile(entries []*WAL_Entry) error {
	// Create a temporary file to make the operation look atomic.
	tempFilePath := fmt.Sprintf("%s.tmp", wal.currentSegment.Name())
	tempFile, err := os.OpenFile(tempFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	// Write the entries to the temporary file
	for _, entry := range entries {
		marshaledEntry := MustMarshal(entry)

		size := int32(len(marshaledEntry))
		if err := binary.Write(tempFile, binary.LittleEndian, size); err != nil {
			return err
		}
		_, err := tempFile.Write(marshaledEntry)

		if err != nil {
			return err
		}
	}

	// Close the temporary file
	if err := tempFile.Close(); err != nil {
		return err
	}

	// Rename the temporary file to the original file name
	// this OS operation is atomic
	if err := os.Rename(tempFilePath, wal.currentSegment.Name()); err != nil {
		return err
	}

	return nil
}

// Returns the last sequence number in the current log segment file.
func (wal *WAL) getLastSequenceNo() (uint64, error) {
	entry, err := wal.getLastEntryInLog()
	if err != nil {
		return 0, err
	}

	if entry != nil {
		return entry.GetLogSequenceNumber(), nil
	}

	return 0, nil
}

// iterates through all the entries of the log and returns the last entry.
func (wal *WAL) getLastEntryInLog() (*WAL_Entry, error) {
	file, err := os.OpenFile(wal.currentSegment.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var previousSize int32
	var offset int64
	var entry *WAL_Entry

	for {
		var size int32
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				// End of file reached, read the last entry at the saved offset.
				if offset == 0 {
					return entry, nil
				}

				// seek back to the last offset from where last entry data is starting
				if _, err := file.Seek(offset, io.SeekStart); err != nil {
					return nil, err
				}

				// Read the entry data.
				data := make([]byte, previousSize)
				if _, err := io.ReadFull(file, data); err != nil {
					return nil, err
				}

				entry, err = unmarshalAndVerifyEntry(data)
				if err != nil {
					return nil, err
				}

				return entry, nil
			}
			return nil, err
		}

		// Get current offset
		offset, err = file.Seek(0, io.SeekCurrent)
		previousSize = size

		if err != nil {
			return nil, err
		}

		// Skip to the next entry.
		if _, err := file.Seek(int64(size), io.SeekCurrent); err != nil {
			return nil, err
		}
	}
}
