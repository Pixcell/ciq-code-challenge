package parser

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"server-log-analyzer/internal/models"
)

// TestParseCSV tests the main CSV parsing functionality
func TestParseCSV(t *testing.T) {
	tests := []struct {
		name        string
		csvContent  string
		wantEntries int
		wantErr     bool
	}{
		{
			name: "valid CSV with UNIX timestamps",
			csvContent: `timestamp,username,operation,size
1587772800,jeff22,upload,45
1587772900,alice42,download,120
1587773000,jeff22,upload,75`,
			wantEntries: 3,
			wantErr:     false,
		},
		{
			name: "valid CSV with human-readable timestamps",
			csvContent: `timestamp,username,operation,size
Sun Apr 12 22:10:38 UTC 2020,sarah94,download,34
Sun Apr 12 22:35:06 UTC 2020,Maia86,download,75
Sun Apr 12 22:49:47 UTC 2020,Maia86,upload,9`,
			wantEntries: 3,
			wantErr:     false,
		},
		{
			name: "CSV without header",
			csvContent: `1587772800,jeff22,upload,45
1587772900,alice42,download,120`,
			wantEntries: 2,
			wantErr:     false,
		},
		{
			name: "empty CSV file",
			csvContent: `timestamp,username,operation,size`,
			wantEntries: 0,
			wantErr:     true,
		},
		{
			name: "invalid operation",
			csvContent: `timestamp,username,operation,size
1587772800,jeff22,delete,45`,
			wantEntries: 0,
			wantErr:     true,
		},
		{
			name: "invalid size",
			csvContent: `timestamp,username,operation,size
1587772800,jeff22,upload,invalid`,
			wantEntries: 0,
			wantErr:     true,
		},
		{
			name: "negative size",
			csvContent: `timestamp,username,operation,size
1587772800,jeff22,upload,-45`,
			wantEntries: 0,
			wantErr:     true,
		},
		{
			name: "empty username",
			csvContent: `timestamp,username,operation,size
1587772800,,upload,45`,
			wantEntries: 0,
			wantErr:     true,
		},
		{
			name: "wrong number of fields",
			csvContent: `timestamp,username,operation,size
1587772800,jeff22,upload`,
			wantEntries: 0,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary file
			tmpFile := createTempCSVFile(t, tt.csvContent)
			defer os.Remove(tmpFile)

			// Parse the CSV
			entries, err := ParseCSV(tmpFile)

			// Check error expectation
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseCSV() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Check number of entries
			if len(entries) != tt.wantEntries {
				t.Errorf("ParseCSV() got %d entries, want %d", len(entries), tt.wantEntries)
			}

			// Additional validation for successful cases
			if !tt.wantErr && len(entries) > 0 {
				validateLogEntries(t, entries)
			}
		})
	}
}

// TestParseCSVFileNotFound tests handling of non-existent files
func TestParseCSVFileNotFound(t *testing.T) {
	_, err := ParseCSV("non_existent_file.csv")
	if err == nil {
		t.Error("ParseCSV() expected error for non-existent file, got nil")
	}
}

// TestParseTimestamp tests the timestamp parsing functionality
func TestParseTimestamp(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantErr   bool
		checkTime bool
	}{
		{
			name:      "valid UNIX timestamp (seconds)",
			input:     "1587772800",
			wantErr:   false,
			checkTime: true,
		},
		{
			name:      "valid UNIX timestamp (milliseconds)",
			input:     "1587772800000",
			wantErr:   false,
			checkTime: true,
		},
		{
			name:      "valid human-readable timestamp with timezone",
			input:     "Sun Apr 12 22:10:38 UTC 2020",
			wantErr:   false,
			checkTime: true,
		},
		{
			name:      "valid human-readable timestamp without timezone",
			input:     "Sun Apr 12 22:10:38 2020",
			wantErr:   false,
			checkTime: true,
		},
		{
			name:    "invalid timestamp format",
			input:   "invalid-timestamp",
			wantErr: true,
		},
		{
			name:    "empty timestamp",
			input:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseTimestamp(tt.input)

			if (err != nil) != tt.wantErr {
				t.Errorf("parseTimestamp() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.checkTime && !tt.wantErr {
				if result.IsZero() {
					t.Error("parseTimestamp() returned zero time for valid input")
				}
				// Verify the timestamp is reasonable (between 2020 and 2030)
				if result.Year() < 2020 || result.Year() > 2030 {
					t.Errorf("parseTimestamp() returned unreasonable year: %d", result.Year())
				}
			}
		})
	}
}

// TestIsValidOperation tests operation validation
func TestIsValidOperation(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		want      bool
	}{
		{"valid upload", "upload", true},
		{"valid download", "download", true},
		{"invalid delete", "delete", false},
		{"invalid empty", "", false},
		{"invalid case", "Upload", false},
		{"invalid with spaces", " upload ", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isValidOperation(tt.operation); got != tt.want {
				t.Errorf("isValidOperation() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestParseSize tests size parsing and validation
func TestParseSize(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int
		wantErr bool
	}{
		{"valid positive integer", "123", 123, false},
		{"valid zero", "0", 0, false},
		{"valid large number", "999999", 999999, false},
		{"invalid negative", "-45", 0, true},
		{"invalid string", "abc", 0, true},
		{"invalid empty", "", 0, true},
		{"invalid float", "12.5", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSize(tt.input)

			if (err != nil) != tt.wantErr {
				t.Errorf("parseSize() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if got != tt.want {
				t.Errorf("parseSize() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestIsHeaderRow tests header row detection
func TestIsHeaderRow(t *testing.T) {
	tests := []struct {
		name   string
		record []string
		want   bool
	}{
		{
			name:   "typical header row",
			record: []string{"timestamp", "username", "operation", "size"},
			want:   true,
		},
		{
			name:   "header with timestamp only",
			record: []string{"timestamp", "user", "op", "bytes"},
			want:   true,
		},
		{
			name:   "header with username",
			record: []string{"time", "username", "action", "bytes"},
			want:   true,
		},
		{
			name:   "header with operation",
			record: []string{"time", "user", "operation", "bytes"},
			want:   true,
		},
		{
			name:   "header with size",
			record: []string{"time", "user", "action", "size"},
			want:   true,
		},
		{
			name:   "data row with numbers",
			record: []string{"1587772800", "jeff22", "upload", "45"},
			want:   false,
		},
		{
			name:   "data row with readable timestamp",
			record: []string{"Sun Apr 12 22:10:38 UTC 2020", "sarah94", "download", "34"},
			want:   false,
		},
		{
			name:   "wrong number of fields",
			record: []string{"timestamp", "username", "operation"},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isHeaderRow(tt.record); got != tt.want {
				t.Errorf("isHeaderRow() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestParseLogEntry tests individual log entry parsing
func TestParseLogEntry(t *testing.T) {
	tests := []struct {
		name    string
		record  []string
		line    int
		wantErr bool
	}{
		{
			name:    "valid entry with UNIX timestamp",
			record:  []string{"1587772800", "jeff22", "upload", "45"},
			line:    1,
			wantErr: false,
		},
		{
			name:    "valid entry with human timestamp",
			record:  []string{"Sun Apr 12 22:10:38 UTC 2020", "sarah94", "download", "34"},
			line:    2,
			wantErr: false,
		},
		{
			name:    "invalid timestamp",
			record:  []string{"invalid", "jeff22", "upload", "45"},
			line:    3,
			wantErr: true,
		},
		{
			name:    "empty username",
			record:  []string{"1587772800", "", "upload", "45"},
			line:    4,
			wantErr: true,
		},
		{
			name:    "invalid operation",
			record:  []string{"1587772800", "jeff22", "delete", "45"},
			line:    5,
			wantErr: true,
		},
		{
			name:    "invalid size",
			record:  []string{"1587772800", "jeff22", "upload", "invalid"},
			line:    6,
			wantErr: true,
		},
		{
			name:    "wrong field count",
			record:  []string{"1587772800", "jeff22", "upload"},
			line:    7,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry, err := parseLogEntry(tt.record, tt.line)

			if (err != nil) != tt.wantErr {
				t.Errorf("parseLogEntry() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				// Validate the parsed entry
				if entry.Username == "" {
					t.Error("parseLogEntry() returned empty username")
				}
				if entry.Operation != "upload" && entry.Operation != "download" {
					t.Errorf("parseLogEntry() invalid operation: %s", entry.Operation)
				}
				if entry.Size < 0 {
					t.Errorf("parseLogEntry() negative size: %d", entry.Size)
				}
				if entry.Timestamp.IsZero() {
					t.Error("parseLogEntry() returned zero timestamp")
				}
			}
		})
	}
}

// Benchmark tests for performance validation
func BenchmarkParseCSV(b *testing.B) {
	// Create a test CSV with multiple entries
	csvContent := `timestamp,username,operation,size
1587772800,jeff22,upload,45
1587772900,alice42,download,120
1587773000,jeff22,upload,75
1587773100,bob15,upload,30
1587773200,alice42,upload,200`

	tmpFile := createTempCSVFile(b, csvContent)
	defer os.Remove(tmpFile)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseCSV(tmpFile)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseTimestamp(b *testing.B) {
	timestamps := []string{
		"1587772800",
		"Sun Apr 12 22:10:38 UTC 2020",
		"1587772800000",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, ts := range timestamps {
			_, err := parseTimestamp(ts)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// Helper functions

// createTempCSVFile creates a temporary CSV file with the given content
func createTempCSVFile(t testing.TB, content string) string {
	tmpFile := filepath.Join(t.TempDir(), "test.csv")
	err := os.WriteFile(tmpFile, []byte(content), 0644)
	if err != nil {
		t.Fatal(err)
	}
	return tmpFile
}

// validateLogEntries performs additional validation on parsed entries
func validateLogEntries(t *testing.T, entries []models.LogEntry) {
	for i, entry := range entries {
		if entry.Username == "" {
			t.Errorf("Entry %d has empty username", i)
		}
		if entry.Operation != "upload" && entry.Operation != "download" {
			t.Errorf("Entry %d has invalid operation: %s", i, entry.Operation)
		}
		if entry.Size < 0 {
			t.Errorf("Entry %d has negative size: %d", i, entry.Size)
		}
		if entry.Timestamp.IsZero() {
			t.Errorf("Entry %d has zero timestamp", i)
		}
		// Validate timestamp is reasonable (between 2020 and 2030)
		if entry.Timestamp.Year() < 2020 || entry.Timestamp.Year() > 2030 {
			t.Errorf("Entry %d has unreasonable timestamp year: %d", i, entry.Timestamp.Year())
		}
	}
}

// Example test - demonstrates expected usage
func ExampleParseCSV() {
	// This would typically use a real file
	// For this example, we'll show the expected behavior

	// Create a simple CSV file (in real usage, this would be your log file)
	tmpFile := "/tmp/example.csv"
	content := `timestamp,username,operation,size
1587772800,jeff22,upload,45
1587772900,alice42,download,120`

	err := os.WriteFile(tmpFile, []byte(content), 0644)
	if err != nil {
		return
	}
	defer os.Remove(tmpFile)

	entries, err := ParseCSV(tmpFile)
	if err != nil {
		return
	}

	// Print first entry details
	if len(entries) > 0 {
		entry := entries[0]
		fmt.Printf("Username: %s\n", entry.Username)
		fmt.Printf("Operation: %s\n", entry.Operation)
		fmt.Printf("Size: %d\n", entry.Size)
	}

	// Output:
	// Username: jeff22
	// Operation: upload
	// Size: 45
}
