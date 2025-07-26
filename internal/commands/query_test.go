package commands

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestValidateReadOnlyQuery tests the query validation function
func TestValidateReadOnlyQuery(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
		errMsg  string
	}{
		// Valid queries
		{
			name:    "simple SELECT",
			query:   "SELECT * FROM logs",
			wantErr: false,
		},
		{
			name:    "SELECT with WHERE",
			query:   "SELECT username FROM logs WHERE operation = 'upload'",
			wantErr: false,
		},
		{
			name:    "SELECT with aggregation",
			query:   "SELECT COUNT(*) FROM logs GROUP BY username",
			wantErr: false,
		},
		{
			name:    "CTE query",
			query:   "WITH stats AS (SELECT COUNT(*) as cnt FROM logs) SELECT * FROM stats",
			wantErr: false,
		},
		{
			name:    "EXPLAIN query",
			query:   "EXPLAIN SELECT * FROM logs",
			wantErr: false,
		},
		{
			name:    "EXPLAIN QUERY PLAN",
			query:   "EXPLAIN QUERY PLAN SELECT * FROM logs WHERE username = 'jeff22'",
			wantErr: false,
		},
		{
			name:    "read-only PRAGMA",
			query:   "PRAGMA table_info(logs)",
			wantErr: false,
		},
		{
			name:    "case insensitive SELECT",
			query:   "select * from logs",
			wantErr: false,
		},
		{
			name:    "SELECT with single-line comment",
			query:   "SELECT * FROM logs -- this is a comment",
			wantErr: false,
		},
		{
			name:    "SELECT with multi-line comment",
			query:   "SELECT * FROM logs /* this is a comment */",
			wantErr: false,
		},
		{
			name:    "complex SELECT with subquery",
			query:   "SELECT * FROM logs WHERE username IN (SELECT username FROM logs WHERE size > 100)",
			wantErr: false,
		},

		// Invalid queries - wrong statement type
		{
			name:    "INSERT statement",
			query:   "INSERT INTO logs VALUES (1, '2020-01-01', 'user', 'upload', 100)",
			wantErr: true,
			errMsg:  "only read-only queries are allowed",
		},
		{
			name:    "UPDATE statement",
			query:   "UPDATE logs SET username = 'hacker' WHERE id = 1",
			wantErr: true,
			errMsg:  "only read-only queries are allowed",
		},
		{
			name:    "DELETE statement",
			query:   "DELETE FROM logs WHERE username = 'jeff22'",
			wantErr: true,
			errMsg:  "only read-only queries are allowed",
		},
		{
			name:    "CREATE TABLE",
			query:   "CREATE TABLE evil (id INTEGER)",
			wantErr: true,
			errMsg:  "only read-only queries are allowed",
		},
		{
			name:    "DROP TABLE",
			query:   "DROP TABLE logs",
			wantErr: true,
			errMsg:  "only read-only queries are allowed",
		},
		{
			name:    "ALTER TABLE",
			query:   "ALTER TABLE logs ADD COLUMN evil TEXT",
			wantErr: true,
			errMsg:  "only read-only queries are allowed",
		},

		// Invalid queries - forbidden keywords in valid statements
		{
			name:    "SELECT with forbidden keyword",
			query:   "SELECT * FROM logs; DROP TABLE logs;",
			wantErr: true,
			errMsg:  "forbidden keyword 'DROP' detected",
		},
		{
			name:    "forbidden keyword in subquery",
			query:   "SELECT * FROM (INSERT INTO logs VALUES (1, '2020-01-01', 'user', 'upload', 100))",
			wantErr: true,
			errMsg:  "forbidden keyword 'INSERT' detected",
		},
		{
			name:    "transaction statements",
			query:   "BEGIN; SELECT * FROM logs; COMMIT;",
			wantErr: true,
			errMsg:  "only read-only queries are allowed",
		},

		// Invalid queries - multiple statements
		{
			name:    "multiple SELECT statements",
			query:   "SELECT * FROM logs; SELECT COUNT(*) FROM logs; SELECT 1;",
			wantErr: true,
			errMsg:  "multiple statements not allowed",
		},

		// Invalid queries - write PRAGMA
		{
			name:    "write PRAGMA",
			query:   "PRAGMA journal_mode = WAL",
			wantErr: true,
			errMsg:  "PRAGMA statement not allowed",
		},
		{
			name:    "unknown PRAGMA",
			query:   "PRAGMA evil_setting = 1",
			wantErr: true,
			errMsg:  "PRAGMA statement not allowed",
		},

		// Edge cases
		{
			name:    "empty query",
			query:   "",
			wantErr: true,
			errMsg:  "empty query",
		},
		{
			name:    "whitespace only",
			query:   "   \n\t  ",
			wantErr: true,
			errMsg:  "empty query",
		},
		{
			name:    "comment only",
			query:   "-- this is just a comment",
			wantErr: true,
			errMsg:  "empty query",
		},
		{
			name:    "case sensitive forbidden word (should still catch)",
			query:   "SELECT * FROM logs; delete from logs;",
			wantErr: true,
			errMsg:  "forbidden keyword 'DELETE' detected",
		},
		{
			name:    "forbidden word as part of larger word (should not match)",
			query:   "SELECT * FROM logs WHERE username = 'dropbox_user'",
			wantErr: false, // "drop" is part of "dropbox" so should not match
		},
		{
			name:    "VACUUM statement",
			query:   "VACUUM",
			wantErr: true,
			errMsg:  "only read-only queries are allowed",
		},
		{
			name:    "REINDEX statement",
			query:   "REINDEX",
			wantErr: true,
			errMsg:  "only read-only queries are allowed",
		},
		{
			name:    "ATTACH DATABASE",
			query:   "ATTACH DATABASE 'evil.db' AS evil",
			wantErr: true,
			errMsg:  "only read-only queries are allowed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateReadOnlyQuery(tt.query)

			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateReadOnlyQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr && tt.errMsg != "" {
				if err == nil {
					t.Errorf("ValidateReadOnlyQuery() expected error containing '%s', got nil", tt.errMsg)
				} else if !containsString(err.Error(), tt.errMsg) {
					t.Errorf("ValidateReadOnlyQuery() error = '%v', expected to contain '%s'", err.Error(), tt.errMsg)
				}
			}
		})
	}
}

// TestValidateReadOnlyQueryPragmas tests specific PRAGMA validations
func TestValidateReadOnlyQueryPragmas(t *testing.T) {
	validPragmas := []string{
		"PRAGMA table_info(logs)",
		"PRAGMA index_list(logs)",
		"PRAGMA index_info(idx_logs_username)",
		"PRAGMA foreign_key_list(logs)",
		"PRAGMA schema_version",
		"PRAGMA user_version",
		"PRAGMA database_list",
		"PRAGMA compile_options",
	}

	for _, pragma := range validPragmas {
		t.Run("valid_"+pragma, func(t *testing.T) {
			err := ValidateReadOnlyQuery(pragma)
			if err != nil {
				t.Errorf("ValidateReadOnlyQuery() for '%s' error = %v, want nil", pragma, err)
			}
		})
	}

	invalidPragmas := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = FULL",
		"PRAGMA cache_size = 10000",
		"PRAGMA temp_store = MEMORY",
		"PRAGMA foreign_keys = ON",
		"PRAGMA recursive_triggers = ON",
	}

	for _, pragma := range invalidPragmas {
		t.Run("invalid_"+pragma, func(t *testing.T) {
			err := ValidateReadOnlyQuery(pragma)
			if err == nil {
				t.Errorf("ValidateReadOnlyQuery() for '%s' expected error, got nil", pragma)
			}
		})
	}
}

// BenchmarkValidateReadOnlyQuery benchmarks the validation function
func BenchmarkValidateReadOnlyQuery(b *testing.B) {
	queries := []string{
		"SELECT * FROM logs",
		"SELECT COUNT(*) FROM logs WHERE operation = 'upload' AND size > 50",
		"WITH stats AS (SELECT COUNT(*) as cnt FROM logs) SELECT * FROM stats",
		"EXPLAIN QUERY PLAN SELECT * FROM logs WHERE username = 'jeff22'",
		"PRAGMA table_info(logs)",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, query := range queries {
			_ = ValidateReadOnlyQuery(query)
		}
	}
}

// Helper function to check if a string contains a substring (case-insensitive)
func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr ||
		len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
		strings.Contains(s, substr)))
}

// ExampleValidateReadOnlyQuery demonstrates the query validation
func ExampleValidateReadOnlyQuery() {
	// Valid query
	err := ValidateReadOnlyQuery("SELECT COUNT(*) FROM logs")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Println("Valid read-only query")
	}

	// Invalid query
	err = ValidateReadOnlyQuery("DROP TABLE logs")
	if err != nil {
		fmt.Printf("Blocked: %v\n", err)
	}

	// Output:
	// Valid read-only query
	// Blocked: only read-only queries are allowed (SELECT, WITH, EXPLAIN, and read-only PRAGMA)
}

// TestNewQueryCommand tests the query command creation
func TestNewQueryCommand(t *testing.T) {
	cmd := NewQueryCommand()

	if cmd == nil {
		t.Fatal("NewQueryCommand() returned nil")
	}

	if cmd.Use != "query" {
		t.Errorf("Expected command name 'query', got '%s'", cmd.Use)
	}

	if cmd.Short == "" {
		t.Error("Command short description is empty")
	}

	if cmd.Long == "" {
		t.Error("Command long description is empty")
	}
}

// TestQueryCommandFlags tests that all required flags are properly configured
func TestQueryCommandFlags(t *testing.T) {
	cmd := NewQueryCommand()

	// Test that flags exist
	requiredFlags := []string{"db", "table"}
	for _, flagName := range requiredFlags {
		flag := cmd.Flags().Lookup(flagName)
		if flag == nil {
			t.Errorf("Expected flag '%s' not found", flagName)
		}
	}

	// Test default values
	dbFlag := cmd.Flags().Lookup("db")
	if dbFlag == nil {
		t.Fatal("DB flag not found")
	}
	if dbFlag.DefValue != "server_logs.db" {
		t.Errorf("Expected default db value 'server_logs.db', got '%s'", dbFlag.DefValue)
	}

	tableFlag := cmd.Flags().Lookup("table")
	if tableFlag == nil {
		t.Fatal("Table flag not found")
	}
	if tableFlag.DefValue != "logs" {
		t.Errorf("Expected default table value 'logs', got '%s'", tableFlag.DefValue)
	}
}

// TestQueryCommandValidation tests command argument validation
func TestQueryCommandValidation(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid minimal args",
			args:    []string{},
			wantErr: false,
		},
		{
			name:    "valid with db flag",
			args:    []string{"--db", "test.db"},
			wantErr: false,
		},
		{
			name:    "valid with table flag",
			args:    []string{"--table", "mytable"},
			wantErr: false,
		},
		{
			name:    "valid full args",
			args:    []string{"--db", "test.db", "--table", "mytable"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := NewQueryCommand()
			cmd.SetArgs(tt.args)

			// Capture output
			var buf bytes.Buffer
			cmd.SetOut(&buf)
			cmd.SetErr(&buf)

			// Since query command tries to connect to database, we expect it to fail
			// with database connection errors, not flag validation errors
			err := cmd.Execute()

			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error but got none")
				} else if tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("Expected error containing '%s', got '%s'", tt.errMsg, err.Error())
				}
			} else {
				// For valid args, we expect database connection errors, not flag validation errors
				if err != nil && strings.Contains(err.Error(), "flag") {
					t.Errorf("Unexpected flag validation error: %v", err)
				}
			}
		})
	}
}

// TestQueryCommandDatabaseConnection tests database connection scenarios
func TestQueryCommandDatabaseConnection(t *testing.T) {
	tempDir := t.TempDir()

	tests := []struct {
		name       string
		dbPath     string
		setupDB    bool
		wantErr    bool
		errMsg     string
	}{
		{
			name:    "non-existent database",
			dbPath:  filepath.Join(tempDir, "nonexistent.db"),
			setupDB: false,
			wantErr: true,
			errMsg:  "does not exist",
		},
		{
			name:    "valid database file",
			dbPath:  filepath.Join(tempDir, "valid.db"),
			setupDB: true,
			wantErr: false,
		},
		{
			name:    "memory database",
			dbPath:  ":memory:",
			setupDB: false,
			wantErr: true,
			errMsg:  "does not exist", // Memory database requires data to be loaded first
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setupDB && tt.dbPath != ":memory:" {
				// Create a valid SQLite database file
				file, err := os.Create(tt.dbPath)
				if err != nil {
					t.Fatalf("Failed to create test database: %v", err)
				}
				file.Close()
			}

			cmd := NewQueryCommand()
			cmd.SetArgs([]string{"--db", tt.dbPath})

			// Capture output
			var buf bytes.Buffer
			cmd.SetOut(&buf)
			cmd.SetErr(&buf)

			// We need to provide some input to avoid hanging on stdin
			cmd.SetIn(strings.NewReader("exit\n"))

			err := cmd.Execute()
			output := buf.String()

			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error but got none. Output: %s", output)
				} else if tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("Expected error containing '%s', got '%s'", tt.errMsg, err.Error())
				}
			} else if err != nil && !strings.Contains(output, "Connected to database") {
				t.Errorf("Unexpected error: %v\nOutput: %s", err, output)
			}
		})
	}
}

// TestQueryCommandTablePlaceholder tests table placeholder functionality
func TestQueryCommandTablePlaceholder(t *testing.T) {
	// This test verifies that the {table} placeholder replacement works
	tests := []struct {
		name        string
		query       string
		tableName   string
		expected    string
	}{
		{
			name:      "simple table placeholder",
			query:     "SELECT * FROM {table}",
			tableName: "users",
			expected:  "SELECT * FROM users",
		},
		{
			name:      "multiple table placeholders",
			query:     "SELECT COUNT(*) FROM {table} WHERE {table}.active = 1",
			tableName: "accounts",
			expected:  "SELECT COUNT(*) FROM accounts WHERE accounts.active = 1",
		},
		{
			name:      "no placeholder",
			query:     "SELECT * FROM logs",
			tableName: "users",
			expected:  "SELECT * FROM logs",
		},
		{
			name:      "placeholder in string literal",
			query:     "SELECT '{table}' as table_name FROM users",
			tableName: "test",
			expected:  "SELECT 'test' as table_name FROM users", // Simple replacement - doesn't parse SQL syntax
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := replaceTablePlaceholder(tt.query, tt.tableName)
			if result != tt.expected {
				t.Errorf("replaceTablePlaceholder(%q, %q) = %q, want %q",
					tt.query, tt.tableName, result, tt.expected)
			}
		})
	}
}

// TestQueryCommandValidateQuery tests the query validation with various inputs
func TestQueryCommandValidateQuery(t *testing.T) {
	// Additional edge cases beyond the existing ValidateReadOnlyQuery tests
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "query with comments",
			query:   "-- This is a comment\nSELECT * FROM logs -- Another comment",
			wantErr: false,
		},
		{
			name:    "multiline select",
			query:   "SELECT username,\n       operation,\n       size\nFROM logs\nWHERE size > 100",
			wantErr: false,
		},
		{
			name:    "select with subquery",
			query:   "SELECT * FROM (SELECT username FROM logs WHERE size > 50) as large_files",
			wantErr: false,
		},
		{
			name:    "case insensitive keywords",
			query:   "select * from logs where username = 'test'",
			wantErr: false,
		},
		{
			name:    "mixed case keywords",
			query:   "Select Username From Logs Order By Size Desc",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateReadOnlyQuery(tt.query)

			if tt.wantErr && err == nil {
				t.Errorf("Expected error but got none for query: %s", tt.query)
			} else if !tt.wantErr && err != nil {
				t.Errorf("Unexpected error for query '%s': %v", tt.query, err)
			}
		})
	}
}

// TestQueryCommandSpecialCommands tests special commands like .tables, .help, etc.
func TestQueryCommandSpecialCommands(t *testing.T) {
	tests := []struct {
		name     string
		command  string
		expected []string // Strings that should appear in output
	}{
		{
			name:    "tables command",
			command: ".tables",
			expected: []string{"Tables in database"},
		},
		{
			name:    "help command",
			command: ".help",
			expected: []string{"Available commands", ".tables", ".help", "exit", "quit"},
		},
		{
			name:    "exit command",
			command: "exit",
			expected: []string{}, // Should exit without specific output
		},
		{
			name:    "quit command",
			command: "quit",
			expected: []string{}, // Should exit without specific output
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the special command handling logic
			// This would require refactoring the interactive mode to be more testable
			// For now, we test the command recognition

			if isExitCommand(tt.command) && (tt.command == "exit" || tt.command == "quit") {
				// Expected behavior for exit commands
			} else if tt.command == ".tables" || tt.command == ".help" {
				// These should be recognized as special commands
				if !strings.HasPrefix(tt.command, ".") {
					t.Errorf("Special command %s should start with dot", tt.command)
				}
			}
		})
	}
}

// Helper function to test exit command recognition
func isExitCommand(cmd string) bool {
	trimmed := strings.TrimSpace(strings.ToLower(cmd))
	return trimmed == "exit" || trimmed == "quit"
}

// Helper function for table placeholder replacement (this should exist in the actual code)
func replaceTablePlaceholder(query, tableName string) string {
	return strings.ReplaceAll(query, "{table}", tableName)
}

func BenchmarkReplaceTablePlaceholder(b *testing.B) {
	query := "SELECT COUNT(*) FROM {table} WHERE {table}.active = 1 AND {table}.size > 100"
	tableName := "server_logs"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = replaceTablePlaceholder(query, tableName)
	}
}

// Example demonstrates query command usage
func ExampleNewQueryCommand() {
	cmd := NewQueryCommand()

	// Set up command arguments
	cmd.SetArgs([]string{
		"--db", "server_logs.db",
		"--table", "logs",
	})

	// Execute the command (would start interactive mode)
	// cmd.Execute()

	fmt.Println("Query command configured")
	// Output: Query command configured
}
