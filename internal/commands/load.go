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
// Usage: server-log-analyzer load --file server_log.csv [--db logs.db] [--append]
func NewLoadCommand() *cobra.Command {
	var csvFile string
	var dbFile string
	var appendMode bool

	cmd := &cobra.Command{
		Use:   "load",
		Short: "Load CSV log file into SQLite database",
		Long: `Parse a CSV log file and store the data in a SQLite database for efficient querying.

The CSV file should have columns in this order: timestamp, username, operation, size
- timestamp: UNIX timestamp
- username: unique user identifier
- operation: "upload" or "download"
- size: file size in kB (integer)

By default, loading data will replace any existing data in the database.
Use the --append flag to add data to an existing database without clearing it.

Example:
  server-log-analyzer load --file server_log.csv --db logs.db
  server-log-analyzer load --file new_data.csv --db logs.db --append`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLoadCommand(csvFile, dbFile, appendMode)
		},
	}

	// Define command flags
	cmd.Flags().StringVarP(&csvFile, "file", "f", "", "Path to CSV log file (required)")
	cmd.Flags().StringVarP(&dbFile, "db", "d", config.DefaultDatabaseFile, config.DatabaseFileDescription)
	cmd.Flags().BoolVar(&appendMode, "append", false, "Append data to existing database (default: replace existing data)")
	cmd.MarkFlagRequired("file")

	return cmd
}

// runLoadCommand executes the CSV loading logic
func runLoadCommand(csvFile, dbFile string, appendMode bool) error {
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
	if appendMode {
		if dbExists {
			fmt.Printf("Mode: Append to existing database\n")
		} else {
			fmt.Printf("Mode: Create new database (append mode requested but DB doesn't exist)\n")
		}
	} else {
		fmt.Printf("Mode: Replace existing data\n")
	}

	// Initialize database connection and create tables
	db, err := database.Initialize(dbFile)
	if err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}
	defer db.Close()

	// Parse CSV file and extract log entries
	entries, err := parser.ParseCSV(csvFile)
	if err != nil {
		return fmt.Errorf("failed to parse CSV file: %w", err)
	}

	fmt.Printf("Parsed %d log entries\n", len(entries))

	// Insert entries into database
	count, err := database.InsertLogEntries(db, entries, appendMode)
	if err != nil {
		return fmt.Errorf("failed to insert log entries: %w", err)
	}

	fmt.Printf("Successfully loaded %d entries into database\n", count)
	return nil
}
