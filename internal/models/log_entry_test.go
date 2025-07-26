package models

import (
	"fmt"
	"testing"
	"time"
)

// TestLogEntry tests the LogEntry struct
func TestLogEntry(t *testing.T) {
	// Test creating a LogEntry
	timestamp := time.Unix(1587772800, 0)
	entry := LogEntry{
		Timestamp: timestamp,
		Username:  "jeff22",
		Operation: "upload",
		Size:      45,
	}

	// Verify all fields are set correctly
	if entry.Timestamp != timestamp {
		t.Errorf("Expected timestamp %v, got %v", timestamp, entry.Timestamp)
	}

	if entry.Username != "jeff22" {
		t.Errorf("Expected username 'jeff22', got '%s'", entry.Username)
	}

	if entry.Operation != "upload" {
		t.Errorf("Expected operation 'upload', got '%s'", entry.Operation)
	}

	if entry.Size != 45 {
		t.Errorf("Expected size 45, got %d", entry.Size)
	}
}

// TestLogEntryZeroValues tests zero values
func TestLogEntryZeroValues(t *testing.T) {
	var entry LogEntry

	if !entry.Timestamp.IsZero() {
		t.Error("Expected zero timestamp")
	}

	if entry.Username != "" {
		t.Error("Expected empty username")
	}

	if entry.Operation != "" {
		t.Error("Expected empty operation")
	}

	if entry.Size != 0 {
		t.Error("Expected zero size")
	}
}

// TestLogEntryComparison tests comparing LogEntry structs
func TestLogEntryComparison(t *testing.T) {
	timestamp := time.Unix(1587772800, 0)

	entry1 := LogEntry{
		Timestamp: timestamp,
		Username:  "jeff22",
		Operation: "upload",
		Size:      45,
	}

	entry2 := LogEntry{
		Timestamp: timestamp,
		Username:  "jeff22",
		Operation: "upload",
		Size:      45,
	}

	entry3 := LogEntry{
		Timestamp: timestamp,
		Username:  "alice42",
		Operation: "download",
		Size:      120,
	}

	// Test equality
	if entry1 != entry2 {
		t.Error("Expected equal entries to be equal")
	}

	// Test inequality
	if entry1 == entry3 {
		t.Error("Expected different entries to be unequal")
	}
}

// TestLogEntryValidOperations tests valid operations
func TestLogEntryValidOperations(t *testing.T) {
	validOps := []string{"upload", "download"}
	timestamp := time.Unix(1587772800, 0)

	for _, op := range validOps {
		entry := LogEntry{
			Timestamp: timestamp,
			Username:  "testuser",
			Operation: op,
			Size:      100,
		}

		if entry.Operation != op {
			t.Errorf("Expected operation '%s', got '%s'", op, entry.Operation)
		}
	}
}

// TestLogEntrySlice tests working with slices of LogEntry
func TestLogEntrySlice(t *testing.T) {
	baseTime := time.Unix(1587772800, 0)

	entries := []LogEntry{
		{
			Timestamp: baseTime,
			Username:  "jeff22",
			Operation: "upload",
			Size:      45,
		},
		{
			Timestamp: baseTime.Add(time.Minute),
			Username:  "alice42",
			Operation: "download",
			Size:      120,
		},
		{
			Timestamp: baseTime.Add(2 * time.Minute),
			Username:  "jeff22",
			Operation: "upload",
			Size:      75,
		},
	}

	if len(entries) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(entries))
	}

	// Test accessing individual entries
	if entries[0].Username != "jeff22" {
		t.Error("First entry username mismatch")
	}

	if entries[1].Operation != "download" {
		t.Error("Second entry operation mismatch")
	}

	if entries[2].Size != 75 {
		t.Error("Third entry size mismatch")
	}
}

// BenchmarkLogEntryCreation benchmarks creating LogEntry structs
func BenchmarkLogEntryCreation(b *testing.B) {
	timestamp := time.Unix(1587772800, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = LogEntry{
			Timestamp: timestamp,
			Username:  "jeff22",
			Operation: "upload",
			Size:      45,
		}
	}
}

// BenchmarkLogEntrySliceAppend benchmarks appending to LogEntry slices
func BenchmarkLogEntrySliceAppend(b *testing.B) {
	timestamp := time.Unix(1587772800, 0)
	entry := LogEntry{
		Timestamp: timestamp,
		Username:  "jeff22",
		Operation: "upload",
		Size:      45,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var entries []LogEntry
		for j := 0; j < 1000; j++ {
			entries = append(entries, entry)
		}
	}
}

// ExampleLogEntry demonstrates basic usage of LogEntry
func ExampleLogEntry() {
	// Create a new log entry
	entry := LogEntry{
		Timestamp: time.Unix(1587772800, 0),
		Username:  "jeff22",
		Operation: "upload",
		Size:      45,
	}

	// Access the fields
	fmt.Printf("User: %s\n", entry.Username)
	fmt.Printf("Operation: %s\n", entry.Operation)
	fmt.Printf("Size: %d kB\n", entry.Size)

	// Output:
	// User: jeff22
	// Operation: upload
	// Size: 45 kB
}
