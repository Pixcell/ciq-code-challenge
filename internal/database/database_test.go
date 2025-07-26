package database

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"server-log-analyzer/internal/models"
)

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
				results, err := ExecuteQuery(db, "SELECT name FROM sqlite_master WHERE type='table' AND name='logs';")
				if err != nil {
					t.Errorf("Failed to query database: %v", err)
				}

				// Should have logs table
				if len(results) != 1 {
					t.Error("Expected logs table to be created")
				}
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
			inserted, err := InsertLogEntries(db, tt.entries, false)

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
	count1, err := InsertLogEntries(db, firstBatch, false)
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
	count2, err := InsertLogEntries(db, secondBatch, true)
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

	count3, err := InsertLogEntries(db, thirdBatch, false)
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
	_, err = InsertLogEntries(db, testEntries, false)
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
			name:    "invalid operation constraint",
			query:   "INSERT INTO logs (timestamp, username, operation, size) VALUES ('2020-04-15 10:00:00', 'test', 'delete', 100)",
			wantErr: true,
		},
		{
			name:    "negative size constraint",
			query:   "INSERT INTO logs (timestamp, username, operation, size) VALUES ('2020-04-15 10:00:00', 'test', 'upload', -100)",
			wantErr: true,
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
		_, err := InsertLogEntries(db, entries, false)
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
	_, err = InsertLogEntries(db, entries, false)
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

	// Insert some test data
	entries := []models.LogEntry{
		{
			Timestamp: time.Unix(1587772800, 0),
			Username:  "jeff22",
			Operation: "upload",
			Size:      45,
		},
	}

	count, err := InsertLogEntries(db, entries, false)
	if err != nil {
		return
	}

	fmt.Printf("Inserted %d entries\n", count)

	// Output:
	// Inserted 1 entries
}
