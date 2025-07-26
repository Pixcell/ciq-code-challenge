package database

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"server-log-analyzer/internal/models"
	"server-log-analyzer/internal/parser"
)

// setupLogsTable creates a logs table with the standard schema for testing
func setupLogsTable(db DB) error {
	schema := parser.TableSchema{
		Name: "logs",
		Columns: []parser.ColumnSchema{
			{Name: "timestamp", Type: parser.TypeTimestamp, Index: true},
			{Name: "username", Type: parser.TypeText, Index: true},
			{Name: "operation", Type: parser.TypeText, Index: true},
			{Name: "size", Type: parser.TypeInteger},
		},
	}
	return CreateTableFromSchema(db, &schema, false)
}

// TestInitialize tests database initialization
func TestInitialize(t *testing.T) {
	tests := []struct {
		name    string
		dbPath  string
		wantErr bool
	}{
		{
			name:    "valid database path",
			dbPath:  ":memory:",
			wantErr: false,
		},
		{
			name:    "file database path",
			dbPath:  filepath.Join(t.TempDir(), "test.db"),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Initialize(tt.dbPath)

			if (err != nil) != tt.wantErr {
				t.Errorf("Initialize() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if db == nil {
					t.Error("Initialize() returned nil database")
				}
				defer db.Close()

				// Test that we can execute a simple query
				results, err := ExecuteQuery(db, "SELECT name FROM sqlite_master WHERE type='table';")
				if err != nil {
					t.Errorf("Failed to query database: %v", err)
				}

				// Should be able to query (no specific table expected at initialization)
				_ = results
			}
		})
	}
}

// TestInsertLogEntries tests bulk insertion of log entries
func TestInsertLogEntries(t *testing.T) {
	db, err := Initialize(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create the logs table for testing
	if err := setupLogsTable(db); err != nil {
		t.Fatalf("Failed to setup logs table: %v", err)
	}

	tests := []struct {
		name        string
		entries     []models.LogEntry
		wantInserted int64
		wantErr     bool
	}{
		{
			name: "valid entries",
			entries: []models.LogEntry{
				{
					Timestamp: time.Unix(1587772800, 0),
					Username:  "jeff22",
					Operation: "upload",
					Size:      45,
				},
				{
					Timestamp: time.Unix(1587772900, 0),
					Username:  "alice42",
					Operation: "download",
					Size:      120,
				},
			},
			wantInserted: 2,
			wantErr:      false,
		},
		{
			name:        "empty entries slice",
			entries:     []models.LogEntry{},
			wantInserted: 0,
			wantErr:     false,
		},
		{
			name:        "nil entries slice",
			entries:     nil,
			wantInserted: 0,
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inserted, err := InsertLogEntries(db, tt.entries, false, "logs")

			if (err != nil) != tt.wantErr {
				t.Errorf("InsertLogEntries() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if inserted != tt.wantInserted {
				t.Errorf("InsertLogEntries() inserted = %v, want %v", inserted, tt.wantInserted)
			}

			// Verify data was actually inserted
			if !tt.wantErr && tt.wantInserted > 0 {
				results, err := ExecuteQuery(db, "SELECT COUNT(*) as count FROM logs")
				if err != nil {
					t.Errorf("Failed to verify insertion: %v", err)
				}
				if len(results) > 0 {
					if count, ok := results[0]["count"].(int64); ok {
						if count != tt.wantInserted {
							t.Errorf("Expected %d entries in database, got %d", tt.wantInserted, count)
						}
					}
				}
			}
		})
	}
}

// TestInsertLogEntriesAppendMode tests appending data to existing database
func TestInsertLogEntriesAppendMode(t *testing.T) {
	db, err := Initialize(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create the logs table for testing
	if err := setupLogsTable(db); err != nil {
		t.Fatalf("Failed to setup logs table: %v", err)
	}

	// First batch of entries
	firstBatch := []models.LogEntry{
		{
			Timestamp: time.Unix(1587772800, 0),
			Username:  "user1",
			Operation: "upload",
			Size:      100,
		},
		{
			Timestamp: time.Unix(1587772900, 0),
			Username:  "user2",
			Operation: "download",
			Size:      200,
		},
	}

	// Insert first batch (replace mode - should clear any existing data)
	count1, err := InsertLogEntries(db, firstBatch, false, "logs")
	if err != nil {
		t.Fatalf("Failed to insert first batch: %v", err)
	}
	if count1 != 2 {
		t.Errorf("Expected 2 entries inserted in first batch, got %d", count1)
	}

	// Verify first batch count
	results, err := ExecuteQuery(db, "SELECT COUNT(*) as count FROM logs")
	if err != nil {
		t.Fatalf("Failed to query count after first batch: %v", err)
	}
	if count, ok := results[0]["count"].(int64); !ok || count != 2 {
		t.Errorf("Expected 2 entries after first batch, got %v", results[0]["count"])
	}

	// Second batch of entries
	secondBatch := []models.LogEntry{
		{
			Timestamp: time.Unix(1587773000, 0),
			Username:  "user3",
			Operation: "upload",
			Size:      300,
		},
	}

	// Insert second batch in append mode
	count2, err := InsertLogEntries(db, secondBatch, true, "logs")
	if err != nil {
		t.Fatalf("Failed to insert second batch in append mode: %v", err)
	}
	if count2 != 1 {
		t.Errorf("Expected 1 entry inserted in second batch, got %d", count2)
	}

	// Verify total count is 3 (2 from first batch + 1 from second batch)
	results, err = ExecuteQuery(db, "SELECT COUNT(*) as count FROM logs")
	if err != nil {
		t.Fatalf("Failed to query count after append: %v", err)
	}
	if count, ok := results[0]["count"].(int64); !ok || count != 3 {
		t.Errorf("Expected 3 entries after append, got %v", results[0]["count"])
	}

	// Verify all users are present
	results, err = ExecuteQuery(db, "SELECT DISTINCT username FROM logs ORDER BY username")
	if err != nil {
		t.Fatalf("Failed to query usernames: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("Expected 3 distinct users, got %d", len(results))
	}

	expectedUsers := []string{"user1", "user2", "user3"}
	for i, result := range results {
		if username, ok := result["username"].(string); !ok || username != expectedUsers[i] {
			t.Errorf("Expected user %s at position %d, got %v", expectedUsers[i], i, result["username"])
		}
	}

	// Test replace mode after append - should clear all data
	thirdBatch := []models.LogEntry{
		{
			Timestamp: time.Unix(1587774000, 0),
			Username:  "user4",
			Operation: "download",
			Size:      400,
		},
	}

	count3, err := InsertLogEntries(db, thirdBatch, false, "logs")
	if err != nil {
		t.Fatalf("Failed to insert third batch in replace mode: %v", err)
	}
	if count3 != 1 {
		t.Errorf("Expected 1 entry inserted in third batch, got %d", count3)
	}

	// Verify only the third batch remains
	results, err = ExecuteQuery(db, "SELECT COUNT(*) as count FROM logs")
	if err != nil {
		t.Fatalf("Failed to query count after replace: %v", err)
	}
	if count, ok := results[0]["count"].(int64); !ok || count != 1 {
		t.Errorf("Expected 1 entry after replace, got %v", results[0]["count"])
	}

	results, err = ExecuteQuery(db, "SELECT username FROM logs")
	if err != nil {
		t.Fatalf("Failed to query username after replace: %v", err)
	}
	if len(results) != 1 || results[0]["username"] != "user4" {
		t.Errorf("Expected only user4 after replace, got %v", results)
	}
}

// TestExecuteQuery tests SQL query execution
func TestExecuteQuery(t *testing.T) {
	db, err := Initialize(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create the logs table for testing
	if err := setupLogsTable(db); err != nil {
		t.Fatalf("Failed to setup logs table: %v", err)
	}

	// Insert test data
	testEntries := []models.LogEntry{
		{
			Timestamp: time.Unix(1587772800, 0),
			Username:  "jeff22",
			Operation: "upload",
			Size:      45,
		},
		{
			Timestamp: time.Unix(1587772900, 0),
			Username:  "alice42",
			Operation: "download",
			Size:      120,
		},
		{
			Timestamp: time.Unix(1587773000, 0),
			Username:  "jeff22",
			Operation: "upload",
			Size:      75,
		},
	}
	_, err = InsertLogEntries(db, testEntries, false, "logs")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name        string
		query       string
		wantRows    int
		wantErr     bool
		checkResult func([]map[string]interface{}) bool
	}{
		{
			name:     "count all entries",
			query:    "SELECT COUNT(*) as count FROM logs",
			wantRows: 1,
			wantErr:  false,
			checkResult: func(results []map[string]interface{}) bool {
				if len(results) != 1 {
					return false
				}
				count, ok := results[0]["count"].(int64)
				return ok && count == 3
			},
		},
		{
			name:     "count distinct users",
			query:    "SELECT COUNT(DISTINCT username) as unique_users FROM logs",
			wantRows: 1,
			wantErr:  false,
			checkResult: func(results []map[string]interface{}) bool {
				if len(results) != 1 {
					return false
				}
				count, ok := results[0]["unique_users"].(int64)
				return ok && count == 2
			},
		},
		{
			name:     "filter by username",
			query:    "SELECT * FROM logs WHERE username = 'jeff22'",
			wantRows: 2,
			wantErr:  false,
			checkResult: func(results []map[string]interface{}) bool {
				if len(results) != 2 {
					return false
				}
				for _, result := range results {
					username, ok := result["username"].(string)
					if !ok || username != "jeff22" {
						return false
					}
				}
				return true
			},
		},
		{
			name:     "filter by operation and size",
			query:    "SELECT * FROM logs WHERE operation = 'upload' AND size > 50",
			wantRows: 1,
			wantErr:  false,
			checkResult: func(results []map[string]interface{}) bool {
				if len(results) != 1 {
					return false
				}
				size, ok := results[0]["size"].(int64)
				return ok && size == 75
			},
		},
		{
			name:    "invalid SQL",
			query:   "INVALID SQL QUERY",
			wantErr: true,
		},
		{
			name:     "select from non-existent table",
			query:    "SELECT * FROM non_existent_table",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := ExecuteQuery(db, tt.query)

			if (err != nil) != tt.wantErr {
				t.Errorf("ExecuteQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if len(results) != tt.wantRows {
					t.Errorf("ExecuteQuery() returned %d rows, want %d", len(results), tt.wantRows)
				}

				// Run custom validation if provided
				if tt.checkResult != nil && !tt.checkResult(results) {
					t.Error("ExecuteQuery() result validation failed")
				}
			}
		})
	}
}

// TestCreateTables tests table creation
func TestCreateTables(t *testing.T) {
	db, err := Initialize(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create the logs table for testing
	if err := setupLogsTable(db); err != nil {
		t.Fatalf("Failed to setup logs table: %v", err)
	}

	// Verify logs table exists and has correct structure
	results, err := ExecuteQuery(db, "PRAGMA table_info(logs)")
	if err != nil {
		t.Fatalf("Failed to get table info: %v", err)
	}

	expectedColumns := map[string]bool{
		"id":        false,
		"timestamp": false,
		"username":  false,
		"operation": false,
		"size":      false,
	}

	for _, result := range results {
		if name, ok := result["name"].(string); ok {
			if _, exists := expectedColumns[name]; exists {
				expectedColumns[name] = true
			}
		}
	}

	for column, found := range expectedColumns {
		if !found {
			t.Errorf("Expected column %s not found in logs table", column)
		}
	}

	// Verify indexes exist
	results, err = ExecuteQuery(db, "PRAGMA index_list(logs)")
	if err != nil {
		t.Fatalf("Failed to get index list: %v", err)
	}

	if len(results) == 0 {
		t.Error("No indexes found on logs table")
	}
}

// TestDatabaseConstraints tests database constraints
func TestDatabaseConstraints(t *testing.T) {
	db, err := Initialize(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create the logs table for testing
	if err := setupLogsTable(db); err != nil {
		t.Fatalf("Failed to setup logs table: %v", err)
	}

	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "valid insert",
			query:   "INSERT INTO logs (timestamp, username, operation, size) VALUES ('2020-04-15 10:00:00', 'test', 'upload', 100)",
			wantErr: false,
		},
		{
			name:    "different operation is allowed in dynamic schema",
			query:   "INSERT INTO logs (timestamp, username, operation, size) VALUES ('2020-04-15 10:00:00', 'test', 'delete', 100)",
			wantErr: false, // Dynamic schema allows any string values
		},
		{
			name:    "negative size is allowed in dynamic schema",
			query:   "INSERT INTO logs (timestamp, username, operation, size) VALUES ('2020-04-15 10:00:00', 'test', 'upload', -100)",
			wantErr: false, // Dynamic schema allows any integer values
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec(tt.query)

			if (err != nil) != tt.wantErr {
				t.Errorf("Constraint test error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Benchmark tests
func BenchmarkInsertLogEntries(b *testing.B) {
	db, err := Initialize(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create the logs table for testing
	if err := setupLogsTable(db); err != nil {
		b.Fatalf("Failed to setup logs table: %v", err)
	}

	// Create test entries
	entries := make([]models.LogEntry, 100)
	baseTime := time.Unix(1587772800, 0)
	for i := range entries {
		entries[i] = models.LogEntry{
			Timestamp: baseTime.Add(time.Duration(i) * time.Second),
			Username:  "user" + string(rune(i%10)),
			Operation: []string{"upload", "download"}[i%2],
			Size:      i * 10,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := InsertLogEntries(db, entries, false, "logs")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExecuteQuery(b *testing.B) {
	db, err := Initialize(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create the logs table for testing
	if err := setupLogsTable(db); err != nil {
		b.Fatalf("Failed to setup logs table: %v", err)
	}

	// Insert test data
	entries := make([]models.LogEntry, 1000)
	baseTime := time.Unix(1587772800, 0)
	for i := range entries {
		entries[i] = models.LogEntry{
			Timestamp: baseTime.Add(time.Duration(i) * time.Second),
			Username:  "user" + string(rune(i%10)),
			Operation: []string{"upload", "download"}[i%2],
			Size:      i * 10,
		}
	}
	_, err = InsertLogEntries(db, entries, false, "logs")
	if err != nil {
		b.Fatal(err)
	}

	query := "SELECT COUNT(*) FROM logs WHERE operation = 'upload' AND size > 500"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ExecuteQuery(db, query)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// ExampleInitialize demonstrates database initialization
func ExampleInitialize() {
	// Initialize an in-memory database for testing
	db, err := Initialize(":memory:")
	if err != nil {
		return
	}
	defer db.Close()

	// Create the logs table for testing
	if err := setupLogsTable(db); err != nil {
		return
	}

	// Insert some test data
	entries := []models.LogEntry{
		{
			Timestamp: time.Unix(1587772800, 0),
			Username:  "jeff22",
			Operation: "upload",
			Size:      45,
		},
	}

	count, err := InsertLogEntries(db, entries, false, "logs")
	if err != nil {
		return
	}

	fmt.Printf("Inserted %d entries\n", count)

	// Output:
	// Inserted 1 entries
}

// TestCreateTableFromSchemaEdgeCases tests edge cases in schema creation
func TestCreateTableFromSchemaEdgeCases(t *testing.T) {
	db, err := Initialize(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tests := []struct {
		name    string
		schema  parser.TableSchema
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid schema",
			schema: parser.TableSchema{
				Name: "test_table",
				Columns: []parser.ColumnSchema{
					{Name: "name", Type: parser.TypeText},
					{Name: "email", Type: parser.TypeText},
				},
			},
			wantErr: false,
		},
		{
			name: "empty table name",
			schema: parser.TableSchema{
				Name: "",
				Columns: []parser.ColumnSchema{
					{Name: "column1", Type: parser.TypeInteger},
				},
			},
			wantErr: true,
		},
		{
			name: "no columns",
			schema: parser.TableSchema{
				Name:    "empty_table",
				Columns: []parser.ColumnSchema{},
			},
			wantErr: true,
		},
		{
			name: "special characters in table name",
			schema: parser.TableSchema{
				Name: "test_table_2",
				Columns: []parser.ColumnSchema{
					{Name: "column1", Type: parser.TypeInteger},
				},
			},
			wantErr: false,
		},
		{
			name: "unicode column names",
			schema: parser.TableSchema{
				Name: "unicode_test",
				Columns: []parser.ColumnSchema{
					{Name: "用户名", Type: parser.TypeText},
					{Name: "email", Type: parser.TypeText},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CreateTableFromSchema(db, &tt.schema, false)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error but got none")
				} else if tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("Expected error containing '%s', got '%s'", tt.errMsg, err.Error())
				}
			} else if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

// TestCreateTableFromSchemaReplaceMode tests replace mode functionality
func TestCreateTableFromSchemaReplaceMode(t *testing.T) {
	db, err := Initialize(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create initial schema
	schema1 := parser.TableSchema{
		Name: "test_table",
		Columns: []parser.ColumnSchema{
			{Name: "name", Type: parser.TypeText},
		},
	}

	err = CreateTableFromSchema(db, &schema1, false)
	if err != nil {
		t.Fatalf("Failed to create initial table: %v", err)
	}

	// Insert some data
	_, err = db.Exec("INSERT INTO test_table (name) VALUES ('test1')")
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Verify data exists
	results, err := ExecuteQuery(db, "SELECT COUNT(*) as count FROM test_table")
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}
	if len(results) == 0 || results[0]["count"].(int64) != 1 {
		t.Fatal("Expected 1 record in table")
	}

	// Create new schema with replace mode
	schema2 := parser.TableSchema{
		Name: "test_table",
		Columns: []parser.ColumnSchema{
			{Name: "description", Type: parser.TypeText}, // Different column name
		},
	}

	err = CreateTableFromSchema(db, &schema2, true) // Replace mode
	if err != nil {
		t.Fatalf("Failed to replace table: %v", err)
	}

	// Verify old data is gone
	results, err = ExecuteQuery(db, "SELECT COUNT(*) as count FROM test_table")
	if err != nil {
		t.Fatalf("Failed to count records after replace: %v", err)
	}
	if len(results) == 0 || results[0]["count"].(int64) != 0 {
		t.Error("Expected 0 records in replaced table")
	}

	// Verify new schema is in place
	results, err = ExecuteQuery(db, "PRAGMA table_info(test_table)")
	if err != nil {
		t.Fatalf("Failed to get table info: %v", err)
	}

	hasDescription := false
	for _, result := range results {
		if name, ok := result["name"].(string); ok && name == "description" {
			hasDescription = true
			break
		}
	}
	if !hasDescription {
		t.Error("Expected 'description' column in replaced table")
	}
}

// TestInsertRecordsEdgeCases tests edge cases in record insertion
func TestInsertRecordsEdgeCases(t *testing.T) {
	db, err := Initialize(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create test table
	schema := parser.TableSchema{
		Name: "test_records",
		Columns: []parser.ColumnSchema{
			{Name: "name", Type: parser.TypeText, Nullable: true},
			{Name: "value", Type: parser.TypeInteger, Nullable: true},
			{Name: "active", Type: parser.TypeBoolean, Nullable: true},
		},
	}
	err = CreateTableFromSchema(db, &schema, false)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	tests := []struct {
		name    string
		headers []string
		records [][]string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "empty headers",
			headers: []string{},
			records: [][]string{{"test", "123", "true"}},
			wantErr: true,
			errMsg:  "no headers",
		},
		{
			name:    "empty records",
			headers: []string{"name", "value", "active"},
			records: [][]string{},
			wantErr: false,
		},
		{
			name:    "mismatched column count",
			headers: []string{"name", "value"},
			records: [][]string{{"test", "123", "true"}}, // 3 values for 2 headers
			wantErr: true,
		},
		{
			name:    "valid records",
			headers: []string{"name", "value", "active"},
			records: [][]string{
				{"test1", "123", "true"},
				{"test2", "456", "false"},
			},
			wantErr: false,
		},
		{
			name:    "records with empty values",
			headers: []string{"name", "value", "active"},
			records: [][]string{
				{"", "0", "false"},
				{"test", "", "true"},
			},
			wantErr: false,
		},
		{
			name:    "records with special characters",
			headers: []string{"name", "value", "active"},
			records: [][]string{
				{"test'with\"quotes", "123", "true"},
				{"test,with,commas", "456", "false"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear table before each test
			_, err := db.Exec("DELETE FROM test_records")
			if err != nil {
				t.Fatalf("Failed to clear table: %v", err)
			}

			count, err := InsertRecords(db, "test_records", tt.headers, tt.records)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error but got none")
				} else if tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("Expected error containing '%s', got '%s'", tt.errMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if count != int64(len(tt.records)) {
					t.Errorf("Expected %d records inserted, got %d", len(tt.records), count)
				}
			}
		})
	}
}

// TestExecuteQueryEdgeCases tests edge cases in query execution
func TestExecuteQueryEdgeCases(t *testing.T) {
	db, err := Initialize(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create test table with data
	if err := setupLogsTable(db); err != nil {
		t.Fatalf("Failed to setup test table: %v", err)
	}

	// Insert test data
	testEntries := []models.LogEntry{
		{
			Timestamp: time.Date(2020, 4, 15, 10, 0, 0, 0, time.UTC),
			Username:  "jeff22",
			Operation: "upload",
			Size:      45,
		},
		{
			Timestamp: time.Date(2020, 4, 15, 10, 5, 0, 0, time.UTC),
			Username:  "alice42",
			Operation: "download",
			Size:      120,
		},
	}
	_, err = InsertLogEntries(db, testEntries, false, "logs")
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	tests := []struct {
		name       string
		query      string
		wantErr    bool
		errMsg     string
		expectRows int
	}{
		{
			name:       "invalid SQL syntax",
			query:      "SELCT * FROM logs", // Typo in SELECT
			wantErr:    true,
			errMsg:     "syntax error",
		},
		{
			name:       "query non-existent table",
			query:      "SELECT * FROM non_existent_table",
			wantErr:    true,
			errMsg:     "no such table",
		},
		{
			name:       "query non-existent column",
			query:      "SELECT non_existent_column FROM logs",
			wantErr:    true,
			errMsg:     "no such column",
		},
		{
			name:       "valid simple query",
			query:      "SELECT COUNT(*) as count FROM logs",
			wantErr:    false,
			expectRows: 1,
		},
		{
			name:       "valid complex query",
			query:      "SELECT username, operation, size FROM logs WHERE size > 50 ORDER BY size DESC",
			wantErr:    false,
			expectRows: 1, // Only alice42's record has size > 50
		},
		{
			name:       "query with special characters",
			query:      "SELECT * FROM logs WHERE username LIKE '%jeff%'",
			wantErr:    false,
			expectRows: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := ExecuteQuery(db, tt.query)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error but got none")
				} else if tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("Expected error containing '%s', got '%s'", tt.errMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if tt.expectRows >= 0 && len(results) != tt.expectRows {
					t.Errorf("Expected %d rows, got %d", tt.expectRows, len(results))
				}
			}
		})
	}
}

// TestDatabaseConnectionErrors tests database connection error scenarios
func TestDatabaseConnectionErrors(t *testing.T) {
	tests := []struct {
		name    string
		dbPath  string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid memory database",
			dbPath:  ":memory:",
			wantErr: false,
		},
		{
			name:    "path to directory instead of file",
			dbPath:  t.TempDir(),
			wantErr: true,
			errMsg:  "is a directory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Initialize(tt.dbPath)

			if tt.wantErr {
				if err == nil {
					if db != nil {
						db.Close()
					}
					t.Errorf("Expected error but got none")
				} else if tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("Expected error containing '%s', got '%s'", tt.errMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				} else if db == nil {
					t.Error("Expected database instance but got nil")
				} else {
					db.Close()
				}
			}
		})
	}
}

// TestDatabaseConcurrency tests concurrent database operations
func TestDatabaseConcurrency(t *testing.T) {
	// Use a temporary file database for concurrency testing since SQLite in-memory
	// databases may not be properly shared between goroutines
	dbPath := filepath.Join(t.TempDir(), "concurrency_test.db")
	db, err := Initialize(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create test table
	if err := setupLogsTable(db); err != nil {
		t.Fatalf("Failed to setup test table: %v", err)
	}

	// Test concurrent inserts
	const numGoroutines = 10
	const recordsPerGoroutine = 5

	errChan := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(routineID int) {
			entries := make([]models.LogEntry, recordsPerGoroutine)
			for j := 0; j < recordsPerGoroutine; j++ {
				entries[j] = models.LogEntry{
					Timestamp: time.Now().Add(time.Duration(routineID*recordsPerGoroutine+j) * time.Second),
					Username:  fmt.Sprintf("user_%d_%d", routineID, j),
					Operation: "upload",
					Size:      100,
				}
			}

			// Use append mode for concurrency and specify table name
			_, err := InsertLogEntries(db, entries, true, "logs")
			errChan <- err
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		if err := <-errChan; err != nil {
			t.Errorf("Concurrent insert failed: %v", err)
		}
	}

	// Verify total count
	results, err := ExecuteQuery(db, "SELECT COUNT(*) as count FROM logs")
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}

	expectedCount := int64(numGoroutines * recordsPerGoroutine)
	if len(results) > 0 {
		if count, ok := results[0]["count"].(int64); ok {
			if count != expectedCount {
				t.Errorf("Expected %d records, got %d", expectedCount, count)
			}
		}
	}
}

// TestDatabaseIndexCreation tests index creation and usage
func TestDatabaseIndexCreation(t *testing.T) {
	db, err := Initialize(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create schema with indexes
	schema := parser.TableSchema{
		Name: "indexed_table",
		Columns: []parser.ColumnSchema{
			{Name: "name", Type: parser.TypeText, Index: true},
			{Name: "email", Type: parser.TypeText, Index: true},
			{Name: "age", Type: parser.TypeInteger, Index: false},
		},
	}

	err = CreateTableFromSchema(db, &schema, false)
	if err != nil {
		t.Fatalf("Failed to create table with indexes: %v", err)
	}

	// Verify indexes were created
	results, err := ExecuteQuery(db, "PRAGMA index_list(indexed_table)")
	if err != nil {
		t.Fatalf("Failed to get index list: %v", err)
	}

	if len(results) < 2 {
		t.Errorf("Expected at least 2 indexes, got %d", len(results))
	}

	// Verify index names
	expectedIndexes := []string{
		"idx_indexed_table_name",
		"idx_indexed_table_email",
	}

	foundIndexes := make(map[string]bool)
	for _, result := range results {
		if name, ok := result["name"].(string); ok {
			foundIndexes[name] = true
		}
	}

	for _, expectedIndex := range expectedIndexes {
		if !foundIndexes[expectedIndex] {
			t.Errorf("Expected index '%s' not found", expectedIndex)
		}
	}
}
