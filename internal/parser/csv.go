// Package parser provides CSV parsing functionality for server log files
package parser

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"server-log-analyzer/internal/models"
)

// ParseCSV reads and parses a CSV file containing server log entries
// Expected CSV format: timestamp, username, operation, size
// - timestamp: UNIX timestamp (integer)
// - username: string
// - operation: "upload" or "download"
// - size: integer (file size in kB)
func ParseCSV(filePath string) ([]models.LogEntry, error) {
	// Open the CSV file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = 4 // Expect exactly 4 fields per record

	var entries []models.LogEntry
	lineNumber := 0

	for {
		// Read the next record
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading CSV at line %d: %w", lineNumber+1, err)
		}

		lineNumber++

		// Skip header row if it exists
		if lineNumber == 1 && isHeaderRow(record) {
			continue
		}

		// Parse the record into a LogEntry
		entry, err := parseLogEntry(record, lineNumber)
		if err != nil {
			return nil, fmt.Errorf("error parsing line %d: %w", lineNumber, err)
		}

		entries = append(entries, entry)
	}

	if len(entries) == 0 {
		return nil, fmt.Errorf("no valid log entries found in CSV file")
	}

	return entries, nil
}

// parseLogEntry converts a CSV record into a LogEntry struct
// Performs validation and type conversion for each field
func parseLogEntry(record []string, lineNumber int) (models.LogEntry, error) {
	if len(record) != 4 {
		return models.LogEntry{}, fmt.Errorf("expected 4 fields, got %d", len(record))
	}

	// Parse timestamp (UNIX timestamp)
	timestampStr := record[0]
	timestamp, err := parseTimestamp(timestampStr)
	if err != nil {
		return models.LogEntry{}, fmt.Errorf("invalid timestamp '%s': %w", timestampStr, err)
	}

	// Parse username (string validation)
	username := record[1]
	if username == "" {
		return models.LogEntry{}, fmt.Errorf("username cannot be empty")
	}

	// Parse operation (validate allowed values)
	operation := record[2]
	if !isValidOperation(operation) {
		return models.LogEntry{}, fmt.Errorf("invalid operation '%s': must be 'upload' or 'download'", operation)
	}

	// Parse size (integer validation)
	sizeStr := record[3]
	size, err := parseSize(sizeStr)
	if err != nil {
		return models.LogEntry{}, fmt.Errorf("invalid size '%s': %w", sizeStr, err)
	}

	return models.LogEntry{
		Timestamp: timestamp,
		Username:  username,
		Operation: operation,
		Size:      size,
	}, nil
}

// parseTimestamp converts a timestamp string to time.Time
// Supports both UNIX timestamps and human-readable formats
func parseTimestamp(timestampStr string) (time.Time, error) {
	// First try to parse as UNIX timestamp
	if timestamp, err := strconv.ParseInt(timestampStr, 10, 64); err == nil {
		// Handle both second and millisecond precision
		// If timestamp > year 2100 in seconds, assume it's milliseconds
		if timestamp > 4102444800 { // January 1, 2100 in seconds
			return time.Unix(timestamp/1000, (timestamp%1000)*1000000), nil
		}
		return time.Unix(timestamp, 0), nil
	}

	// Try to parse as human-readable timestamp
	// Format: "Sun Apr 12 22:10:38 UTC 2020"
	if t, err := time.Parse("Mon Jan 2 15:04:05 MST 2006", timestampStr); err == nil {
		return t, nil
	}

	// Try alternative format without timezone
	if t, err := time.Parse("Mon Jan 2 15:04:05 2006", timestampStr); err == nil {
		return t, nil
	}

	// Try RFC3339 format
	if t, err := time.Parse(time.RFC3339, timestampStr); err == nil {
		return t, nil
	}

	return time.Time{}, fmt.Errorf("timestamp format not recognized, expected UNIX timestamp or 'Mon Jan 2 15:04:05 MST 2006' format")
}

// isValidOperation checks if the operation is either "upload" or "download"
// This validation ensures data consistency and could be extended for new operations
func isValidOperation(operation string) bool {
	return operation == "upload" || operation == "download"
}

// parseSize converts a size string to integer and validates it's non-negative
// File sizes should be non-negative integers representing kB
func parseSize(sizeStr string) (int, error) {
	size, err := strconv.Atoi(sizeStr)
	if err != nil {
		return 0, fmt.Errorf("size must be a valid integer: %w", err)
	}

	if size < 0 {
		return 0, fmt.Errorf("size cannot be negative")
	}

	return size, nil
}

// isHeaderRow checks if the given record appears to be a header row
// This helps automatically skip CSV headers
func isHeaderRow(record []string) bool {
	if len(record) != 4 {
		return false
	}

	// Check if first field looks like "timestamp" header
	if record[0] == "timestamp" {
		return true
	}

	// Check if it contains typical header words
	for _, field := range record {
		if field == "username" || field == "operation" || field == "size" {
			return true
		}
	}

	return false
}

// Future extensions could include:
// - Support for different CSV formats (custom delimiters, headers)
// - Streaming parser for very large files
// - Data validation rules (e.g., reasonable timestamp ranges)
// - Support for compressed CSV files
// - Parallel processing for multiple CSV files
