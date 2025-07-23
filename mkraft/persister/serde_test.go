package persister

import (
	"encoding/binary"
	"slices"
	"testing"
)

func TestBatchSerialize(t *testing.T) {
	serde := NewRaftSerdeImpl()
	entries := []*RaftLogEntry{
		{Term: 1, Commands: []byte("log1")},
		{Term: 2, Commands: []byte("log2")},
		{Term: 3, Commands: []byte("log3")},
	}
	_, err := serde.BatchSerialize(entries)
	if err != nil {
		t.Fatalf("failed to serialize log entries: %v", err)
	}
}

func TestBatchDeserialize(t *testing.T) {
	serde := NewRaftSerdeImpl()
	entries := []*RaftLogEntry{
		{Term: 1, Commands: []byte("log1")},
		{Term: 2, Commands: []byte("log2")},
		{Term: 3, Commands: []byte("log3")},
	}
	serialized, err := serde.BatchSerialize(entries)
	if err != nil {
		t.Fatalf("failed to serialize log entries: %v", err)
	}
	deserialized, err := serde.BatchDeserialize(serialized)
	if err != nil {
		t.Fatalf("failed to deserialize log entries: %v", err)
	}
	t.Logf("deserialized: %v", deserialized)

	// Test error cases
	_, err = serde.BatchDeserialize(nil)
	if err == nil {
		t.Error("expected error for nil data")
	}

	_, err = serde.BatchDeserialize([]byte("invalid data"))
	if err == nil {
		t.Error("expected error for invalid data")
	}

	// Test corrupted data
	corruptedData := slices.Clone(serialized)
	corruptedData[len(corruptedData)-1] = 0xFF
	_, err = serde.BatchDeserialize(corruptedData)
	if err == nil {
		t.Error("expected error for corrupted data")
	}
}

func TestLogSerialize(t *testing.T) {
	serde := NewRaftSerdeImpl()
	entry := &RaftLogEntry{
		Term:     1,
		Commands: []byte("log1"),
	}
	serialized, err := serde.LogSerialize(entry)
	if err != nil {
		t.Fatalf("failed to serialize log entry: %v", err)
	}
	t.Logf("serialized: %v", serialized)

	// Test error cases
	_, err = serde.LogSerialize(nil)
	if err == nil {
		t.Error("expected error for nil entry")
	}
}

func TestLogDeserialize(t *testing.T) {
	serde := NewRaftSerdeImpl()
	entry := &RaftLogEntry{
		Term:     1,
		Commands: []byte("log1"),
	}
	serialized, err := serde.LogSerialize(entry)
	if err != nil {
		t.Fatalf("failed to serialize log entry: %v", err)
	}
	_, err = serde.LogDeserialize(serialized)
	if err != nil {
		t.Fatalf("failed to deserialize log entry: %v", err)
	}
	// t.Logf("deserialized: %v", deserialized)

	// Test error cases
	_, err = serde.LogDeserialize(nil)
	if err == nil {
		t.Error("expected error for nil data")
	}

	_, err = serde.LogDeserialize([]byte("invalid data"))
	if err == nil {
		t.Error("expected error for invalid data")
	}

	// Test corrupted data
	// single log doesn't use crc, we only use crc for batch
	// Test various corruption scenarios

	// Test truncated data
	truncatedData := serialized[:len(serialized)-5]
	_, err = serde.LogDeserialize(truncatedData)
	if err == nil {
		t.Error("expected error for truncated data")
	}

	// Test empty data
	_, err = serde.LogDeserialize([]byte{})
	if err == nil {
		t.Error("expected error for empty data")
	}

	// Test data that's too short
	shortData := make([]byte, 7) // Less than required 8 byte header
	_, err = serde.LogDeserialize(shortData)
	if err == nil {
		t.Error("expected error for data that's too short")
	}

	// Test data with invalid command length
	invalidLenData := make([]byte, len(serialized))
	copy(invalidLenData, serialized)
	// Set command length to larger than remaining data
	binary.BigEndian.PutUint32(invalidLenData[4:8], uint32(len(serialized)))
	_, err = serde.LogDeserialize(invalidLenData)
	if err == nil {
		t.Error("expected error for invalid command length")
	}
}
