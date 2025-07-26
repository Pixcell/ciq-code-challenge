package parser

import (
	"fmt"
	"os"
	"testing"
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
Sun Apr 12 22:10:38 UTC 2020,jeff22,upload,45
Sun Apr 12 22:15:00 UTC 2020,alice42,download,120`,
			wantEntries: 2,
			wantErr:     false,
		},
		{
			name: "empty CSV file",
			csvContent: ``,
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
				t.Errorf("ParseCSV() returned %d entries, want %d", len(entries), tt.wantEntries)
			}
		})
	}
}

// TestParseTimestamp tests timestamp parsing functionality
func TestParseTimestamp(t *testing.T) {
	tests := []struct {
		name      string
		timestamp string
		wantErr   bool
	}{
		{
			name:      "valid UNIX timestamp (seconds)",
			timestamp: "1587504638",
			wantErr:   false,
		},
		{
			name:      "valid human-readable timestamp",
			timestamp: "Sun Apr 12 22:10:38 UTC 2020",
			wantErr:   false,
		},
		{
			name:      "invalid timestamp format",
			timestamp: "2020-04-12 22:10:38",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseTimestamp(tt.timestamp)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseTimestamp() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestIsValidOperation tests operation validation
func TestIsValidOperation(t *testing.T) {
	tests := []struct {
		operation string
		expected  bool
	}{
		{"upload", true},
		{"download", true},
		{"delete", false},
		{"", false},
		{"UPLOAD", false},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("operation_%s", tt.operation), func(t *testing.T) {
			result := isValidOperation(tt.operation)
			if result != tt.expected {
				t.Errorf("isValidOperation(%q) = %v, want %v", tt.operation, result, tt.expected)
			}
		})
	}
}

// Helper function to create a temporary CSV file for testing
func createTempCSVFile(t *testing.T, content string) string {
	tmpFile, err := os.CreateTemp("", "test_*.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatal(err)
	}

	return tmpFile.Name()
}

// Benchmark test
func BenchmarkParseCSV(b *testing.B) {
	csvContent := `timestamp,username,operation,size
1587772800,jeff22,upload,45
1587772900,alice42,download,120
1587773000,jeff22,upload,75`

	tmpFile := createTempCSVFileB(b, csvContent)
	defer os.Remove(tmpFile)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseCSV(tmpFile)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Helper function for benchmarks
func createTempCSVFileB(b *testing.B, content string) string {
	tmpFile, err := os.CreateTemp("", "benchmark_*.csv")
	if err != nil {
		b.Fatal(err)
	}
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		b.Fatal(err)
	}

	return tmpFile.Name()
}

// Example function
func ExampleParseCSV() {
	tmpfile, err := os.CreateTemp("", "example_*.csv")
	if err != nil {
		panic(err)
	}
	defer os.Remove(tmpfile.Name())

	csvContent := `timestamp,username,operation,size
1587772800,jeff22,upload,45
1587772900,alice42,download,120`

	tmpfile.WriteString(csvContent)
	tmpfile.Close()

	entries, err := ParseCSV(tmpfile.Name())
	if err != nil {
		panic(err)
	}

	fmt.Printf("Parsed %d entries\n", len(entries))
	if len(entries) > 0 {
		entry := entries[0]
		fmt.Printf("Username: %s\n", entry.Username)
		fmt.Printf("Operation: %s\n", entry.Operation)
		fmt.Printf("Size: %d\n", entry.Size)
	}

	// Output:
	// Parsed 2 entries
	// Username: jeff22
	// Operation: upload
	// Size: 45
}
