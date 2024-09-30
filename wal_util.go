package wal

import (
	"fmt"
	"github.com/ashwaniYDV/my-wal/types"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// unmarshals the given data into a WAL entry and verifies CRC of the entry.
// Only returns an error if the CRC is invalid.
func unmarshalAndVerifyEntry(data []byte) (*types.WAL_Entry, error) {
	var entry types.WAL_Entry
	MustUnmarshal(data, &entry)

	if !verifyCRC(&entry) {
		return nil, fmt.Errorf("CRC mismatch: data may be corrupted")
	}

	return &entry, nil
}

// Validates whether the given entry has a valid CRC.
func verifyCRC(entry *types.WAL_Entry) bool {
	// Reset the entry CRC for the verification.
	actualCRC := crc32.ChecksumIEEE(append(entry.GetData(), byte(entry.GetLogSequenceNumber())))

	return entry.CRC == actualCRC
}

// Finds the last segment ID from the given list of files.
func findLastSegmentIndexInFiles(files []string) (int, error) {
	var lastSegmentID int
	for _, file := range files {
		_, fileName := filepath.Split(file)
		segmentID, err := strconv.Atoi(strings.TrimPrefix(fileName, "segment-"))
		if err != nil {
			return 0, err
		}
		if segmentID > lastSegmentID {
			lastSegmentID = segmentID
		}
	}
	return lastSegmentID, nil
}

// Creates a log segment file with the given segment ID in the given directory.
func createSegmentFile(directory string, segmentID int) (*os.File, error) {
	filePath := filepath.Join(directory, fmt.Sprintf("segment-%d", segmentID))
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	return file, nil
}
