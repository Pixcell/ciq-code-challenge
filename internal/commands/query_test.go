package commands

import (
	"fmt"
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
