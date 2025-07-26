package parser

import (
	"strings"
	"testing"
)

func TestDetectSchema(t *testing.T) {
	tests := []struct {
		name        string
		headers     []string
		records     [][]string
		tableName   string
		expectCols  int
		expectTypes map[string]ColumnType
		expectIndex map[string]bool
	}{
		{
			name:      "simple log data",
			headers:   []string{"timestamp", "username", "operation", "size"},
			records:   [][]string{
				{"1587504638", "user1", "upload", "100"},
				{"1587504639", "user2", "download", "50"},
				{"1587504640", "user1", "upload", "200"},
			},
			tableName: "logs",
			expectCols: 4,
			expectTypes: map[string]ColumnType{
				"timestamp": TypeTimestamp, // UNIX timestamps are detected as timestamps
				"username":  TypeText,
				"operation": TypeText,
				"size":      TypeInteger,
			},
			expectIndex: map[string]bool{
				"timestamp": true,
				"username":  true,
				"operation": true,
				"size":      false,
			},
		},
		{
			name:      "mixed data types",
			headers:   []string{"id", "name", "score", "active", "created_at"},
			records:   [][]string{
				{"1", "Alice", "95.5", "true", "2023-01-01 10:00:00"},
				{"2", "Bob", "87.2", "false", "2023-01-02 11:30:00"},
				{"3", "Charlie", "92.8", "true", "2023-01-03 09:15:00"},
			},
			tableName: "users",
			expectCols: 5,
			expectTypes: map[string]ColumnType{
				"id":         TypeInteger,
				"name":       TypeText,
				"score":      TypeReal,
				"active":     TypeBoolean,
				"created_at": TypeTimestamp,
			},
			expectIndex: map[string]bool{
				"id":         true,
				"name":       false,
				"score":      false,
				"active":     false,
				"created_at": true, // Contains "created" so should be indexed
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := DetectSchema(tt.headers, tt.records, tt.tableName)
			if err != nil {
				t.Fatalf("DetectSchema() error = %v", err)
			}

			if schema.Name != tt.tableName {
				t.Errorf("Schema name = %v, want %v", schema.Name, tt.tableName)
			}

			if len(schema.Columns) != tt.expectCols {
				t.Errorf("Number of columns = %v, want %v", len(schema.Columns), tt.expectCols)
			}

			for _, col := range schema.Columns {
				if expectedType, exists := tt.expectTypes[col.Name]; exists {
					if col.Type != expectedType {
						t.Errorf("Column %s type = %v, want %v", col.Name, col.Type, expectedType)
					}
				}

				if expectedIndex, exists := tt.expectIndex[col.Name]; exists {
					if col.Index != expectedIndex {
						t.Errorf("Column %s index = %v, want %v", col.Name, col.Index, expectedIndex)
					}
				}
			}
		})
	}
}

func TestInferValueType(t *testing.T) {
	tests := []struct {
		value    string
		expected ColumnType
	}{
		// Integers
		{"123", TypeInteger},
		{"0", TypeInteger},
		{"-456", TypeInteger},

		// Floats
		{"123.45", TypeReal},
		{"0.0", TypeReal},
		{"-67.89", TypeReal},

		// Booleans
		{"true", TypeBoolean},
		{"false", TypeBoolean},
		{"TRUE", TypeBoolean},
		{"FALSE", TypeBoolean},
		{"1", TypeInteger}, // Note: "1" is treated as integer, not boolean
		{"0", TypeInteger}, // Note: "0" is treated as integer, not boolean
		{"yes", TypeBoolean},
		{"no", TypeBoolean},
		{"Y", TypeBoolean},
		{"N", TypeBoolean},

		// Timestamps
		{"1587504638", TypeTimestamp},        // UNIX timestamp
		{"1587504638000", TypeTimestamp},     // UNIX timestamp (milliseconds)
		{"2023-01-01", TypeTimestamp},        // Date
		{"2023-01-01 10:00:00", TypeTimestamp}, // Datetime
		{"2023-01-01T10:00:00Z", TypeTimestamp}, // RFC3339

		// Text
		{"hello", TypeText},
		{"user123", TypeText},
		{"", TypeText},
		{"not-a-number", TypeText},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			result := inferValueType(tt.value)
			if result != tt.expected {
				t.Errorf("inferValueType(%q) = %v, want %v", tt.value, result, tt.expected)
			}
		})
	}
}

func TestIsTimestamp(t *testing.T) {
	tests := []struct {
		value    string
		expected bool
	}{
		// Valid timestamps
		{"1587504638", true},                  // UNIX timestamp
		{"1587504638000", true},              // UNIX timestamp (ms)
		{"2023-01-01", true},                 // Date
		{"2023-01-01 10:00:00", true},        // Datetime
		{"2023-01-01T10:00:00Z", true},       // RFC3339
		{"Sun Apr 12 22:10:38 UTC 2020", true}, // Current parser format

		// Invalid timestamps
		{"not-a-timestamp", false},
		{"123", false},                       // Too short for reasonable timestamp
		{"99999999999999", false},           // Too large
		{"hello world", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			result := isTimestamp(tt.value)
			if result != tt.expected {
				t.Errorf("isTimestamp(%q) = %v, want %v", tt.value, result, tt.expected)
			}
		})
	}
}

func TestSanitizeColumnName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"timestamp", "timestamp"},
		{"User Name", "user_name"},
		{"operation-type", "operation_type"},
		{"file.size", "file_size"},
		{"path/to/file", "path_to_file"},
		{"123column", "col_123column"},
		{"", "unnamed_column"},
		{"HTTP-Code", "http_code"},
		{"Response.Time", "response_time"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := sanitizeColumnName(tt.input)
			if result != tt.expected {
				t.Errorf("sanitizeColumnName(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestShouldIndex(t *testing.T) {
	tests := []struct {
		columnName string
		expected   bool
	}{
		// Should be indexed
		{"timestamp", true},
		{"username", true},
		{"user_id", true},
		{"operation", true},
		{"status_code", true},
		{"ip_address", true},

		// Should not be indexed
		{"description", false},
		{"content", false},
		{"message", false},
		{"size", false},
		{"value", false},
	}

	for _, tt := range tests {
		t.Run(tt.columnName, func(t *testing.T) {
			result := shouldIndex(tt.columnName)
			if result != tt.expected {
				t.Errorf("shouldIndex(%q) = %v, want %v", tt.columnName, result, tt.expected)
			}
		})
	}
}

func TestGenerateCreateTableSQL(t *testing.T) {
	schema := &TableSchema{
		Name: "test_table",
		Columns: []ColumnSchema{
			{Name: "id", Type: TypeInteger, Nullable: false, Index: true},
			{Name: "name", Type: TypeText, Nullable: false, Index: false},
			{Name: "score", Type: TypeReal, Nullable: true, Index: false},
		},
	}

	sql := schema.GenerateCreateTableSQL()

	// Check that it contains expected elements
	expectedParts := []string{
		"CREATE TABLE IF NOT EXISTS test_table",
		"id INTEGER PRIMARY KEY AUTOINCREMENT",
		"id INTEGER NOT NULL",
		"name TEXT NOT NULL",
		"score REAL",
	}

	for _, part := range expectedParts {
		if !strings.Contains(sql, part) {
			t.Errorf("Generated SQL missing expected part: %q\nSQL: %s", part, sql)
		}
	}
}

func TestGenerateIndexSQL(t *testing.T) {
	schema := &TableSchema{
		Name: "test_table",
		Columns: []ColumnSchema{
			{Name: "id", Type: TypeInteger, Nullable: false, Index: true},
			{Name: "name", Type: TypeText, Nullable: false, Index: false},
			{Name: "timestamp", Type: TypeTimestamp, Nullable: false, Index: true},
		},
	}

	indexStatements := schema.GenerateIndexSQL()

	if len(indexStatements) != 2 {
		t.Errorf("Expected 2 index statements, got %d", len(indexStatements))
	}

	expectedStatements := []string{
		"CREATE INDEX IF NOT EXISTS idx_test_table_id ON test_table (id)",
		"CREATE INDEX IF NOT EXISTS idx_test_table_timestamp ON test_table (timestamp)",
	}

	for _, expected := range expectedStatements {
		found := false
		for _, stmt := range indexStatements {
			if stmt == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Missing expected index statement: %q", expected)
		}
	}
}

// Helper function to check if a string contains a substring
func TestContainsHelper(t *testing.T) {
	// This test exists to verify our contains helper works
	tests := []struct {
		s, substr string
		expected  bool
	}{
		{"hello world", "world", true},
		{"hello world", "foo", false},
		{"", "", true},
		{"hello", "", true},
	}

	for _, tt := range tests {
		result := strings.Contains(tt.s, tt.substr)
		if result != tt.expected {
			t.Errorf("strings.Contains(%q, %q) = %v, want %v", tt.s, tt.substr, result, tt.expected)
		}
	}
}
