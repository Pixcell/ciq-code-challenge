// Package config provides shared configuration constants and settings
// for the server log analyzer application
package config

const (
	// DefaultDatabaseFile is the default SQLite database filename
	// used by both load and query commands when no --db flag is provided
	DefaultDatabaseFile = "server_logs.db"

	// DatabaseFileDescription is the help text description for the database file flag
	DatabaseFileDescription = "Path to SQLite database file"
)
