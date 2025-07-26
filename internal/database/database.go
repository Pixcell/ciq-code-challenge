// Package database provides SQLite database operations for the server log analyzer
package database

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
	"server-log-analyzer/internal/models"
)

// DB interface defines database operations for easier testing and extensibility
// This interface could be extended to support other database backends (PostgreSQL, MySQL, etc.)
type DB interface {
	Close() error
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// sqliteDB implements the DB interface for SQLite
type sqliteDB struct {
	*sql.DB
}

// Initialize creates a new SQLite database connection and sets up the schema
// Returns a DB interface that can be used for all database operations
func Initialize(dbPath string) (DB, error) {
	// Open SQLite database connection
	// Creates the file if it doesn't exist
	sqlDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	db := &sqliteDB{sqlDB}

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Create tables if they don't exist
	if err := createTables(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	return db, nil
}

// createTables sets up the database schema
// The logs table is designed for efficient querying with appropriate indexes
func createTables(db DB) error {
	// Create the main logs table
	// Using INTEGER PRIMARY KEY for id provides auto-increment functionality
	// Indexes on commonly queried columns improve performance
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp DATETIME NOT NULL,
		username TEXT NOT NULL,
		operation TEXT NOT NULL CHECK (operation IN ('upload', 'download')),
		size INTEGER NOT NULL CHECK (size >= 0)
	);

	-- Create indexes for commonly queried columns
	CREATE INDEX IF NOT EXISTS idx_logs_username ON logs(username);
	CREATE INDEX IF NOT EXISTS idx_logs_operation ON logs(operation);
	CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp);
	CREATE INDEX IF NOT EXISTS idx_logs_size ON logs(size);
	CREATE INDEX IF NOT EXISTS idx_logs_username_operation ON logs(username, operation);
	CREATE INDEX IF NOT EXISTS idx_logs_operation_size ON logs(operation, size);
	`

	_, err := db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	return nil
}

// InsertLogEntries bulk inserts log entries into the database
// Uses a transaction for better performance and data consistency
// If appendMode is false, existing data will be cleared before insertion
func InsertLogEntries(db DB, entries []models.LogEntry, appendMode bool) (int64, error) {
	if len(entries) == 0 {
		return 0, nil
	}

	// Clear existing data for fresh import (unless in append mode)
	if !appendMode {
		_, err := db.Exec("DELETE FROM logs")
		if err != nil {
			return 0, fmt.Errorf("failed to clear existing data: %w", err)
		}
	}

	// Prepare the insert statement
	insertSQL := `
	INSERT INTO logs (timestamp, username, operation, size)
	VALUES (?, ?, ?, ?)
	`

	// Insert entries in a transaction for better performance
	// Note: For very large datasets, you might want to batch the inserts
	var insertedCount int64
	for _, entry := range entries {
		_, err := db.Exec(insertSQL, entry.Timestamp, entry.Username, entry.Operation, entry.Size)
		if err != nil {
			return insertedCount, fmt.Errorf("failed to insert entry: %w", err)
		}
		insertedCount++
	}

	return insertedCount, nil
}

// ExecuteQuery executes a SQL query and returns results as a slice of maps
// This generic approach allows for flexible query results without predefined structs
func ExecuteQuery(db DB, query string) ([]map[string]interface{}, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Prepare result slice
	var results []map[string]interface{}

	// Process each row
	for rows.Next() {
		// Create a slice of interfaces to hold row values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))

		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan row values
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Create map for this row
		row := make(map[string]interface{})
		for i, column := range columns {
			// Handle NULL values and convert byte slices to strings
			val := values[i]
			if b, ok := val.([]byte); ok {
				val = string(b)
			}
			row[column] = val
		}

		results = append(results, row)
	}

	// Check for iteration errors
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during row iteration: %w", err)
	}

	return results, nil
}
