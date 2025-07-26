package models

import (
	"encoding/json"
	"fmt"
	"strings"
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

// TestLogEntry_String tests the String() method comprehensively
func TestLogEntry_String(t *testing.T) {
	tests := []struct {
		name     string
		entry    LogEntry
		expected string
	}{
		{
			name: "typical upload entry",
			entry: LogEntry{
				ID:        1,
				Timestamp: time.Date(2020, 4, 15, 10, 0, 0, 0, time.UTC),
				Username:  "jeff22",
				Operation: "upload",
				Size:      45,
			},
			expected: "2020-04-15 10:00:00: jeff22 upload 45kB",
		},
		{
			name: "download entry",
			entry: LogEntry{
				ID:        2,
				Timestamp: time.Date(2020, 4, 15, 10, 5, 30, 0, time.UTC),
				Username:  "alice42",
				Operation: "download",
				Size:      120,
			},
			expected: "2020-04-15 10:05:30: alice42 download 120kB",
		},
		{
			name: "zero size entry",
			entry: LogEntry{
				ID:        3,
				Timestamp: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				Username:  "user123",
				Operation: "upload",
				Size:      0,
			},
			expected: "2023-01-01 00:00:00: user123 upload 0kB",
		},
		{
			name: "large file entry",
			entry: LogEntry{
				ID:        4,
				Timestamp: time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
				Username:  "poweruser",
				Operation: "download",
				Size:      999999,
			},
			expected: "2024-12-31 23:59:59: poweruser download 999999kB",
		},
		{
			name: "entry with special characters in username",
			entry: LogEntry{
				ID:        5,
				Timestamp: time.Date(2022, 6, 15, 12, 30, 45, 0, time.UTC),
				Username:  "user_with-dots.123",
				Operation: "upload",
				Size:      42,
			},
			expected: "2022-06-15 12:30:45: user_with-dots.123 upload 42kB",
		},
		{
			name: "empty username",
			entry: LogEntry{
				Timestamp: time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
				Username:  "",
				Operation: "upload",
				Size:      100,
			},
			expected: "2022-01-01 00:00:00:  upload 100kB",
		},
		{
			name: "empty operation",
			entry: LogEntry{
				Timestamp: time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
				Username:  "user123",
				Operation: "",
				Size:      100,
			},
			expected: "2022-01-01 00:00:00: user123  100kB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.entry.String()
			if result != tt.expected {
				t.Errorf("LogEntry.String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

// TestLogEntry_JSONSerialization tests JSON marshaling and unmarshaling
func TestLogEntry_JSONSerialization(t *testing.T) {
	entry := LogEntry{
		ID:        1,
		Timestamp: time.Date(2020, 4, 15, 10, 0, 0, 0, time.UTC),
		Username:  "jeff22",
		Operation: "upload",
		Size:      45,
	}

	// Test JSON marshaling
	jsonData, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("Failed to marshal LogEntry to JSON: %v", err)
	}

	// Verify JSON contains expected fields
	jsonStr := string(jsonData)
	expectedFields := []string{
		`"id":1`,
		`"username":"jeff22"`,
		`"operation":"upload"`,
		`"size":45`,
		`"timestamp":"2020-04-15T10:00:00Z"`,
	}

	for _, field := range expectedFields {
		if !strings.Contains(jsonStr, field) {
			t.Errorf("JSON output missing expected field: %s\nActual JSON: %s", field, jsonStr)
		}
	}

	// Test JSON unmarshaling
	var unmarshaled LogEntry
	err = json.Unmarshal(jsonData, &unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal JSON to LogEntry: %v", err)
	}

	// Verify all fields are correctly unmarshaled
	if unmarshaled.ID != entry.ID {
		t.Errorf("ID mismatch: got %d, want %d", unmarshaled.ID, entry.ID)
	}
	if unmarshaled.Username != entry.Username {
		t.Errorf("Username mismatch: got %s, want %s", unmarshaled.Username, entry.Username)
	}
	if unmarshaled.Operation != entry.Operation {
		t.Errorf("Operation mismatch: got %s, want %s", unmarshaled.Operation, entry.Operation)
	}
	if unmarshaled.Size != entry.Size {
		t.Errorf("Size mismatch: got %d, want %d", unmarshaled.Size, entry.Size)
	}
	if !unmarshaled.Timestamp.Equal(entry.Timestamp) {
		t.Errorf("Timestamp mismatch: got %v, want %v", unmarshaled.Timestamp, entry.Timestamp)
	}
}

// TestLogEntry_StructTags tests that struct tags work correctly
func TestLogEntry_StructTags(t *testing.T) {
	// Test that struct tags are properly defined for database and JSON serialization
	entry := LogEntry{}

	// Test JSON serialization works (which uses the json tags)
	jsonData, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("Failed to marshal empty LogEntry: %v", err)
	}

	// Verify that the zero-value entry produces valid JSON
	var result map[string]interface{}
	err = json.Unmarshal(jsonData, &result)
	if err != nil {
		t.Fatalf("Failed to unmarshal LogEntry JSON: %v", err)
	}

	// Check that expected JSON keys exist
	expectedKeys := []string{"id", "timestamp", "username", "operation", "size"}
	for _, key := range expectedKeys {
		if _, exists := result[key]; !exists {
			t.Errorf("Expected JSON key %s not found in serialized LogEntry", key)
		}
	}
}

// TestLogEntry_EdgeCases tests edge cases and boundary conditions
func TestLogEntry_EdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		entry LogEntry
	}{
		{
			name: "negative ID",
			entry: LogEntry{
				ID:        -1,
				Timestamp: time.Now(),
				Username:  "user123",
				Operation: "upload",
				Size:      100,
			},
		},
		{
			name: "very old timestamp",
			entry: LogEntry{
				Timestamp: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				Username:  "user123",
				Operation: "upload",
				Size:      100,
			},
		},
		{
			name: "future timestamp",
			entry: LogEntry{
				Timestamp: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC),
				Username:  "user123",
				Operation: "upload",
				Size:      100,
			},
		},
		{
			name: "unicode username",
			entry: LogEntry{
				Timestamp: time.Now(),
				Username:  "用户123",
				Operation: "upload",
				Size:      100,
			},
		},
		{
			name: "max int size",
			entry: LogEntry{
				Timestamp: time.Now(),
				Username:  "user123",
				Operation: "upload",
				Size:      2147483647, // max int32
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test that String() method doesn't panic with edge cases
			result := tt.entry.String()
			if result == "" {
				t.Errorf("String() returned empty string for edge case")
			}

			// Test that JSON serialization works with edge cases
			_, err := json.Marshal(tt.entry)
			if err != nil {
				t.Errorf("JSON marshaling failed for edge case: %v", err)
			}
		})
	}
}

// Benchmark the String() method
func BenchmarkLogEntry_String(b *testing.B) {
	entry := LogEntry{
		ID:        1,
		Timestamp: time.Date(2020, 4, 15, 10, 0, 0, 0, time.UTC),
		Username:  "jeff22",
		Operation: "upload",
		Size:      45,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = entry.String()
	}
}

// Benchmark JSON marshaling
func BenchmarkLogEntry_JSONMarshal(b *testing.B) {
	entry := LogEntry{
		ID:        1,
		Timestamp: time.Date(2020, 4, 15, 10, 0, 0, 0, time.UTC),
		Username:  "jeff22",
		Operation: "upload",
		Size:      45,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(entry)
	}
}
