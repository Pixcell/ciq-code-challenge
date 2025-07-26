// Package commands implements the CLI commands for the server log analyzer
package commands

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"server-log-analyzer/internal/config"
	"server-log-analyzer/internal/database"
	"server-log-analyzer/internal/parser"
)

// NewLoadCommand creates the 'load' subcommand for importing CSV data into SQLite
// Usage: server-log-analyzer load --file server_log.csv [--db logs.db] [--table logs] [--append] [--no-schema-detection]
func NewLoadCommand() *cobra.Command {
	var csvFile string
	var dbFile string
	var tableName string
	var appendMode bool
	var schemaDetection bool

	cmd := &cobra.Command{
		Use:   "load",
		Short: "Load CSV log file into SQLite database",
		Long: `Parse a CSV log file and store the data in a SQLite database for efficient querying.

Schema Detection (default: enabled):
When schema detection is enabled, the tool automatically analyzes the CSV file to:
- Detect column names from headers
- Infer data types (TEXT, INTEGER, REAL, DATETIME, BOOLEAN)
- Create appropriate indexes on commonly queried columns

Legacy Mode (--no-schema-detection):
Uses the original fixed schema expecting columns: timestamp, username, operation, size
- timestamp: UNIX timestamp
- username: unique user identifier
- operation: "upload" or "download"
- size: file size in kB (integer)

By default, loading data will replace any existing data in the specified table.
Use the --append flag to add data to an existing table without clearing it.

Examples:
  # Load with automatic schema detection
  server-log-analyzer load --file access_logs.csv --table access_logs

  # Load using legacy schema
  server-log-analyzer load --file server_log.csv --no-schema-detection

  # Load multiple files into different tables
  server-log-analyzer load --file users.csv --table users
  server-log-analyzer load --file errors.csv --table errors --append

  # Append to existing table
  server-log-analyzer load --file new_data.csv --table logs --append`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLoadCommand(csvFile, dbFile, tableName, appendMode, schemaDetection)
		},
	}

	// Define command flags
	cmd.Flags().StringVarP(&csvFile, "file", "f", "", "Path to CSV log file (required)")
	cmd.Flags().StringVarP(&dbFile, "db", "d", config.DefaultDatabaseFile, config.DatabaseFileDescription)
	cmd.Flags().StringVarP(&tableName, "table", "t", config.DefaultTableName, config.TableNameDescription)
	cmd.Flags().BoolVar(&appendMode, "append", false, "Append data to existing table (default: replace existing data)")
	cmd.Flags().BoolVar(&schemaDetection, "schema-detection", true, config.SchemaDetectionDescription)
	cmd.MarkFlagRequired("file")

	return cmd
}

// runLoadCommand executes the CSV loading logic with support for dynamic schema detection
func runLoadCommand(csvFile, dbFile, tableName string, appendMode, schemaDetection bool) error {
	// Validate input file exists
	if _, err := os.Stat(csvFile); os.IsNotExist(err) {
		return fmt.Errorf("CSV file does not exist: %s", csvFile)
	}

	// Check if database exists when in append mode
	dbExists := true
	if _, err := os.Stat(dbFile); os.IsNotExist(err) {
		dbExists = false
		if appendMode {
			fmt.Printf("Warning: Database file does not exist: %s\n", dbFile)
			fmt.Printf("A new database will be created.\n")
		}
	}

	fmt.Printf("Loading CSV file: %s\n", csvFile)
	fmt.Printf("Target database: %s\n", dbFile)
	fmt.Printf("Target table: %s\n", tableName)
	fmt.Printf("Schema detection: %t\n", schemaDetection)

	if appendMode {
		if dbExists {
			fmt.Printf("Mode: Append to existing table\n")
		} else {
			fmt.Printf("Mode: Create new database (append mode requested but DB doesn't exist)\n")
		}
	} else {
		fmt.Printf("Mode: Replace existing data\n")
	}

	var db database.DB
	var err error

	if schemaDetection {
		// Parse CSV for schema detection
		headers, records, err := parser.ParseCSVRaw(csvFile)
		if err != nil {
			return fmt.Errorf("failed to parse CSV file: %w", err)
		}

		if len(records) == 0 {
			return fmt.Errorf("no data found in CSV file")
		}

		// Detect schema from CSV data
		schema, err := parser.DetectSchema(headers, records, tableName)
		if err != nil {
			return fmt.Errorf("failed to detect schema: %w", err)
		}

		// Print detected schema for user confirmation
		printDetectedSchema(schema, len(records))

		// Initialize database connection
		db, err = database.Initialize(dbFile)
		if err != nil {
			return fmt.Errorf("failed to initialize database: %w", err)
		}
		defer db.Close()

		// Create table from schema (skip in append mode to preserve existing structure)
		if !appendMode {
			if err := database.CreateTableFromSchema(db, schema, true); err != nil {
				return fmt.Errorf("failed to create table: %w", err)
			}
		} else {
			// In append mode, create table if it doesn't exist, but don't drop it
			if err := database.CreateTableFromSchema(db, schema, false); err != nil {
				return fmt.Errorf("failed to create table: %w", err)
			}
		}

		// Insert records using dynamic schema
		count, err := database.InsertRecords(db, tableName, headers, records)
		if err != nil {
			return fmt.Errorf("failed to insert records: %w", err)
		}

		fmt.Printf("Successfully loaded %d records into table '%s'\n", count, tableName)
	} else {
		// Legacy mode - use fixed schema
		fmt.Printf("Using legacy schema mode\n")

		// Initialize database connection with legacy schema
		db, err = database.InitializeWithLegacySchema(dbFile)
		if err != nil {
			return fmt.Errorf("failed to initialize database: %w", err)
		}
		defer db.Close()

		// Parse CSV file using legacy parser
		entries, err := parser.ParseCSV(csvFile)
		if err != nil {
			return fmt.Errorf("failed to parse CSV file: %w", err)
		}

		fmt.Printf("Parsed %d log entries\n", len(entries))

		// Insert entries into database using legacy method
		count, err := database.InsertLogEntries(db, entries, appendMode, tableName)
		if err != nil {
			return fmt.Errorf("failed to insert log entries: %w", err)
		}

		fmt.Printf("Successfully loaded %d entries into table '%s'\n", count, tableName)
	}

	return nil
}

// printDetectedSchema displays the detected schema information to the user
func printDetectedSchema(schema *parser.TableSchema, recordCount int) {
	fmt.Printf("\nDetected schema for table '%s' (analyzed %d records):\n", schema.Name, recordCount)
	fmt.Println("┌─────────────────────────┬─────────────┬─────────┐")
	fmt.Println("│ Column                  │ Type        │ Indexed │")
	fmt.Println("├─────────────────────────┼─────────────┼─────────┤")

	for _, col := range schema.Columns {
		indexed := ""
		if col.Index {
			indexed = "✓"
		}
		fmt.Printf("│ %-23s │ %-11s │ %-7s │\n",
			truncateString(col.Name, 23), col.Type.SQLType(), indexed)
	}
	fmt.Println("└─────────────────────────┴─────────────┴─────────┘")
	fmt.Println()
}

// truncateString truncates a string to a maximum length with ellipsis
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
