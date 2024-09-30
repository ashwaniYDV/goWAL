```go
package main

import (
	"encoding/json"
	"fmt"
	wal "github.com/ashwaniYDV/goWAL"
	"os"
	"time"
)

type OperationType int

const (
	InsertOperation OperationType = iota
	DeleteOperation
)

type Record struct {
	Op    OperationType `json:"op"`
	Key   string        `json:"key"`
	Value []byte        `json:"value"`
}

const (
	dirPath     = "./data"
	maxSegments = 3
	maxFileSize = 64 * 1000 * 1000 // 64MB
)

func writeUtil(entry Record, writeAheadLog *wal.WAL) {
	marshaledEntry, err := json.Marshal(entry)
	err = writeAheadLog.WriteEntry(marshaledEntry)
	if err != nil {
		fmt.Println("Failed to write data entry")
		return
	}
}

func checkpointUtil(entry Record, writeAheadLog *wal.WAL) {
	marshaledEntry, err := json.Marshal(entry)
	err = writeAheadLog.CreateCheckpoint(marshaledEntry)
	if err != nil {
		fmt.Println("Failed to write checkpoint entry")
		return
	}
}

func printRecoveredEntries(writeAheadLog *wal.WAL, readFromCheckpoint bool) {
	// enable WAL to sync from buffered memory to disc (syncInterval = 200 * time.Millisecond)
	time.Sleep(2 * time.Second)

	// Recover entries from WAL
	recoveredEntries, err := writeAheadLog.ReadAll(readFromCheckpoint)

	if err != nil {
		fmt.Println("Failed to read recovered entries")
	}

	for _, entry := range recoveredEntries {
		unMarshalledEntry := Record{}
		err := json.Unmarshal(entry.Data, &unMarshalledEntry)
		if err != nil {
			fmt.Println("Failed to unmarshal entry")
			return
		}

		fmt.Println("Key:", string(unMarshalledEntry.Key))
	}
	fmt.Println()
}

func main() {
	defer os.RemoveAll(dirPath) // Cleanup after the test

	writeAheadLog, err := wal.OpenWAL(dirPath, true, maxFileSize, maxSegments)
	defer writeAheadLog.Close()

	if err != nil {
		panic(err)
	}

	// Test data
	entries := []Record{
		{Key: "key1", Value: []byte("value1"), Op: InsertOperation},
		{Key: "key2", Value: []byte("value2"), Op: InsertOperation},
		{Key: "key3", Op: DeleteOperation},
	}

	// Write entries to WAL
	for _, entry := range entries {
		writeUtil(entry, writeAheadLog)
	}

	printRecoveredEntries(writeAheadLog, false)

	checkpointUtil(Record{Key: "cp1", Value: []byte("cp1"), Op: InsertOperation}, writeAheadLog)
	writeUtil(Record{Key: "key4", Value: []byte("value4"), Op: InsertOperation}, writeAheadLog)

	printRecoveredEntries(writeAheadLog, false)

	printRecoveredEntries(writeAheadLog, true)

}
```

Output
```
Key: key1
Key: key2
Key: key3

Key: key1
Key: key2
Key: key3
Key: cp1
Key: key4

Key: cp1
Key: key4
```
