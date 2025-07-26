// Package parser provides CSV parsing functionality for server log files
package parser

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"server-log-analyzer/internal/models"
)

// ParseCSVRaw reads and parses a CSV file returning headers and raw string records
// This function is used for dynamic schema detection
func ParseCSVRaw(filePath string) ([]string, [][]string, error) {
	// Open the CSV file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1 // Allow variable number of fields for flexibility

	var headers []string
	var records [][]string
	lineNumber := 0

	for {
		// Read the next record
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("error reading CSV at line %d: %w", lineNumber+1, err)
		}

		lineNumber++

		// First line determines headers
		if lineNumber == 1 {
			if isHeaderRow(record) {
				headers = record
				continue
			} else {
				// Generate headers if no header row detected
				headers = make([]string, len(record))
				for i := range headers {
					headers[i] = fmt.Sprintf("column_%d", i+1)
				}
				// Include this record as data
				records = append(records, record)
			}
		} else {
			records = append(records, record)
		}
	}

	if len(headers) == 0 {
		return nil, nil, fmt.Errorf("no headers found in CSV file")
	}

	return headers, records, nil
}

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
	if len(record) == 0 {
		return false
	}

	// If length is exactly 4, use legacy logic for backward compatibility
	if len(record) == 4 {
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

		// For 4-field records, apply more stringent header detection
		// Look for common header words and patterns
		headerLikeCount := 0
		for _, field := range record {
			if isCommonHeaderWord(field) {
				headerLikeCount++
			}
		}
		// Need at least 2 clear header words for 4-field legacy format
		return headerLikeCount >= 2
	}

	// For non-4-field records, use generic detection
	if len(record) != 4 {
		headerLikeCount := 0
		for _, field := range record {
			if looksLikeHeader(field) {
				headerLikeCount++
			}
		}
		// If more than half the fields look like headers, treat as header row
		return float64(headerLikeCount)/float64(len(record)) > 0.5
	}

	return false
}

// looksLikeHeader determines if a field looks like a column header
func looksLikeHeader(field string) bool {
	field = strings.TrimSpace(field)
	if field == "" {
		return false
	}

	// Exclude obvious data patterns
	if isPurelyNumeric(field) {
		return false
	}

	// Exclude timestamp-like patterns
	if isTimestampLike(field) {
		return false
	}

	// Exclude email-like patterns
	if strings.Contains(field, "@") {
		return false
	}

	// Headers should be relatively short and contain letters
	if len(field) > 50 {
		return false
	}

	// Must contain letters
	if !containsLetters(field) {
		return false
	}

	// Check if it's a known header word
	if isCommonHeaderWord(field) {
		return true
	}

	// Check for header-like patterns (contains underscore, all lowercase/uppercase)
	if strings.Contains(field, "_") && len(field) <= 20 {
		return true
	}

	// Must be short and look like a typical header
	return len(field) <= 15 && !strings.ContainsAny(field, "0123456789") &&
		   (strings.ToLower(field) == field || strings.ToUpper(field) == field)
}

// isTimestampLike checks if a field looks like a timestamp
func isTimestampLike(field string) bool {
	// Check for common timestamp patterns
	if strings.Contains(field, ":") && (strings.Contains(field, " ") || strings.Contains(field, "T")) {
		return true
	}

	// Check for month names
	months := []string{"Jan", "Feb", "Mar", "Apr", "May", "Jun",
					   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}
	for _, month := range months {
		if strings.Contains(field, month) {
			return true
		}
	}

	return false
}

// isCommonHeaderWord checks if a field is a common header word
func isCommonHeaderWord(field string) bool {
	common := []string{"id", "name", "email", "age", "date", "time", "timestamp",
					   "username", "password", "status", "type", "code", "ip",
					   "address", "method", "path", "operation", "size"}
	lower := strings.ToLower(field)
	for _, word := range common {
		if lower == word || strings.Contains(lower, word) {
			return true
		}
	}
	return false
}

// containsLetters checks if a string contains alphabetic characters
func containsLetters(s string) bool {
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			return true
		}
	}
	return false
}

// isPurelyNumeric checks if a string is purely numeric (including decimals)
func isPurelyNumeric(s string) bool {
	if s == "" {
		return false
	}
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

// Future extensions could include:
// - Support for different CSV formats (custom delimiters, headers)
// - Streaming parser for very large files
// - Data validation rules (e.g., reasonable timestamp ranges)
// - Support for compressed CSV files
// - Parallel processing for multiple CSV files
