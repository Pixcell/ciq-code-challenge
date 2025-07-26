// Package config provides shared configuration constants and settings
// for the server log analyzer application
package config

const (
	// DefaultDatabaseFile is the default SQLite database filename
	// used by both load and query commands when no --db flag is provided
	DefaultDatabaseFile = "server_logs.db"

	// DatabaseFileDescription is the help text description for the database file flag
	DatabaseFileDescription = "Path to SQLite database file"

	// DefaultTableName is the default table name for storing log data
	DefaultTableName = "logs"

	// TableNameDescription is the help text description for the table name flag
	TableNameDescription = "Table name to use for storing/querying data"

	// SchemaDetectionDescription is the help text description for the schema detection flag
	SchemaDetectionDescription = "Enable automatic schema detection from CSV headers and data types"

	// Schema detection settings
	SchemaDetectionSampleSize = 1000
	TypeInferenceThreshold    = 0.8 // 80% of values must match for type assignment
)
