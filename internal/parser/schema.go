// Package parser provides CSV parsing and schema detection functionality
package parser

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"server-log-analyzer/internal/config"
)

// ColumnType represents the detected data type for a CSV column
type ColumnType int

const (
	TypeText ColumnType = iota
	TypeInteger
	TypeReal
	TypeTimestamp
	TypeBoolean
)

// String returns the string representation of ColumnType
func (ct ColumnType) String() string {
	switch ct {
	case TypeInteger:
		return "INTEGER"
	case TypeReal:
		return "REAL"
	case TypeTimestamp:
		return "TIMESTAMP"
	case TypeBoolean:
		return "BOOLEAN"
	default:
		return "TEXT"
	}
}

// SQLType returns the SQLite type string for the column type
func (ct ColumnType) SQLType() string {
	switch ct {
	case TypeInteger:
		return "INTEGER"
	case TypeReal:
		return "REAL"
	case TypeTimestamp:
		return "DATETIME"
	case TypeBoolean:
		return "BOOLEAN"
	default:
		return "TEXT"
	}
}

// ColumnSchema represents the schema for a single column
type ColumnSchema struct {
	Name     string
	Type     ColumnType
	Nullable bool
	Index    bool // Whether to create an index on this column
}

// TableSchema represents the complete schema for a table
type TableSchema struct {
	Name    string
	Columns []ColumnSchema
}

// DetectSchema analyzes CSV data to determine appropriate database schema
// It examines headers and a sample of records to infer column types and indexing needs
func DetectSchema(headers []string, records [][]string, tableName string) (*TableSchema, error) {
	if len(headers) == 0 {
		return nil, fmt.Errorf("no headers found")
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("no data records found")
	}

	schema := &TableSchema{
		Name:    tableName,
		Columns: make([]ColumnSchema, len(headers)),
	}

	// Initialize columns with headers
	for i, header := range headers {
		schema.Columns[i] = ColumnSchema{
			Name:     sanitizeColumnName(header),
			Type:     TypeText, // Default to text
			Nullable: false,
			Index:    shouldIndex(header), // Index common query columns
		}
	}

	// Analyze sample of records to determine types
	sampleSize := min(len(records), config.SchemaDetectionSampleSize)

	for i := range schema.Columns {
		detectedType := detectColumnType(records, i, sampleSize)
		schema.Columns[i].Type = detectedType
	}

	return schema, nil
}

// detectColumnType analyzes values in a column to determine the most appropriate data type
func detectColumnType(records [][]string, columnIndex int, sampleSize int) ColumnType {
	if len(records) == 0 || columnIndex >= len(records[0]) {
		return TypeText
	}

	typeVotes := make(map[ColumnType]int)
	totalValues := 0

	for i := 0; i < sampleSize && i < len(records); i++ {
		if columnIndex >= len(records[i]) {
			continue
		}

		value := strings.TrimSpace(records[i][columnIndex])
		if value == "" {
			continue
		}

		detectedType := inferValueType(value)
		typeVotes[detectedType]++
		totalValues++
	}

	// Return the most common type if it meets the threshold
	return getMostCommonType(typeVotes, totalValues)
}

// inferValueType examines a single value and returns the most specific type it could represent
func inferValueType(value string) ColumnType {
	// Try integer first (before boolean to handle "0" and "1" as integers)
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		// Check if it's a reasonable timestamp
		if isTimestamp(value) {
			return TypeTimestamp
		}
		return TypeInteger
	}

	// Try float
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return TypeReal
	}

	// Try boolean (after numeric types)
	if isBoolean(value) {
		return TypeBoolean
	}

	// Try timestamp last (for non-numeric formats)
	if isTimestamp(value) {
		return TypeTimestamp
	}

	return TypeText
}

// isTimestamp checks if a value looks like a timestamp in various common formats
func isTimestamp(value string) bool {
	// Try UNIX timestamp first (most common in log files)
	if timestamp, err := strconv.ParseInt(value, 10, 64); err == nil {
		// Reasonable timestamp range: 1980-2050 (more restrictive)
		// Minimum: January 1, 1980 = 315532800
		// Maximum: January 1, 2050 = 2524608000
		if timestamp >= 315532800 && timestamp <= 2524608000 {
			return true
		}
		// Handle millisecond timestamps (13 digits)
		if timestamp >= 315532800000 && timestamp <= 2524608000000 {
			return true
		}
	}

	timestampFormats := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.000",
		"01/02/2006 15:04:05",
		"2006-01-02",
		"Mon Jan 2 15:04:05 MST 2006", // Current parser format
		"Mon Jan 2 15:04:05 2006",
	}

	for _, format := range timestampFormats {
		if _, err := time.Parse(format, value); err == nil {
			return true
		}
	}
	return false
}

// isBoolean checks if a value represents a boolean
func isBoolean(value string) bool {
	lower := strings.ToLower(value)
	return lower == "true" || lower == "false" ||
		   lower == "yes" || lower == "no" ||
		   lower == "y" || lower == "n"
}

// getMostCommonType returns the most frequently detected type if it meets the confidence threshold
func getMostCommonType(votes map[ColumnType]int, totalValues int) ColumnType {
	if totalValues == 0 {
		return TypeText
	}

	maxVotes := 0
	commonType := TypeText

	for cType, count := range votes {
		if count > maxVotes {
			maxVotes = count
			commonType = cType
		}
	}

	// Only use the detected type if it meets the confidence threshold
	confidence := float64(maxVotes) / float64(totalValues)
	if confidence >= config.TypeInferenceThreshold {
		return commonType
	}

	return TypeText
}

// sanitizeColumnName cleans up column names to be SQL-safe
func sanitizeColumnName(name string) string {
	// Replace spaces and special characters with underscores
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ReplaceAll(name, ".", "_")
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, "\\", "_")

	// Remove other problematic characters
	name = strings.ToLower(name)

	// Ensure it doesn't start with a number
	if len(name) > 0 && name[0] >= '0' && name[0] <= '9' {
		name = "col_" + name
	}

	// Ensure it's not empty
	if name == "" {
		name = "unnamed_column"
	}

	return name
}

// shouldIndex determines if a column should be automatically indexed based on its name
func shouldIndex(columnName string) bool {
	lower := strings.ToLower(columnName)

	// Exact matches or word boundaries for specific terms
	exactMatches := []string{"id", "timestamp", "time", "date", "username", "user", "operation", "action", "method", "type", "status", "code", "ip", "host", "domain"}
	for _, match := range exactMatches {
		if lower == match {
			return true
		}
	}

	// Suffix/prefix patterns
	if strings.HasSuffix(lower, "_id") || strings.HasPrefix(lower, "id_") {
		return true
	}
	if strings.HasSuffix(lower, "_code") || strings.HasPrefix(lower, "code_") || strings.Contains(lower, "status_code") {
		return true
	}
	if strings.Contains(lower, "user_") || strings.Contains(lower, "_user") {
		return true
	}
	if strings.Contains(lower, "time") || strings.Contains(lower, "date") {
		return true
	}
	if strings.Contains(lower, "created") || strings.Contains(lower, "updated") {
		return true
	}
	if strings.Contains(lower, "ip_") || strings.Contains(lower, "_ip") || strings.HasSuffix(lower, "_address") {
		return true
	}

	return false
}

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// GenerateCreateTableSQL generates the SQL CREATE TABLE statement for the detected schema
func (ts *TableSchema) GenerateCreateTableSQL() string {
	var columns []string

	// Add auto-increment ID column
	columns = append(columns, "id INTEGER PRIMARY KEY AUTOINCREMENT")

	for _, col := range ts.Columns {
		colDef := fmt.Sprintf("%s %s", col.Name, col.Type.SQLType())
		if !col.Nullable {
			colDef += " NOT NULL"
		}
		columns = append(columns, colDef)
	}

	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s\n)",
		ts.Name,
		strings.Join(columns, ",\n  "))
}

// GenerateIndexSQL generates the SQL statements to create indexes for marked columns
func (ts *TableSchema) GenerateIndexSQL() []string {
	var indexStatements []string

	for _, col := range ts.Columns {
		if col.Index {
			indexSQL := fmt.Sprintf(
				"CREATE INDEX IF NOT EXISTS idx_%s_%s ON %s (%s)",
				ts.Name, col.Name, ts.Name, col.Name,
			)
			indexStatements = append(indexStatements, indexSQL)
		}
	}

	return indexStatements
}
