// Package commands implements the CLI commands for the server log analyzer
package commands

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
	"server-log-analyzer/internal/config"
	"server-log-analyzer/internal/database"
)

// NewQueryCommand creates the 'query' subcommand for executing SQL queries
// Usage: server-log-analyzer query [--db logs.db] [--table logs] [--sql "SELECT * FROM logs"]
func NewQueryCommand() *cobra.Command {
	var dbFile string
	var tableName string
	var sqlQuery string

	cmd := &cobra.Command{
		Use:   "query",
		Short: "Execute SQL queries against the log database",
		Long: `Execute SQL queries against the SQLite database containing log data.

You can either provide a query directly via the --sql flag or enter interactive mode
to execute multiple queries. The --table flag sets the default table context but
does not restrict queries to that table.

SECURITY: Only read-only queries are allowed. Write operations (INSERT, UPDATE, DELETE,
CREATE, DROP, etc.) are blocked for data protection.

Common example queries:
  # Count unique users (using default table)
  SELECT COUNT(DISTINCT username) as unique_users FROM {table};

  # Count uploads larger than 50kB
  SELECT COUNT(*) as large_uploads FROM {table} WHERE operation = 'upload' AND size > 50;

  # Count jeff22's uploads on specific date
  SELECT COUNT(*) as jeffs_uploads FROM {table}
  WHERE username = 'jeff22' AND operation = 'upload'
  AND date(timestamp) = '2020-04-15';

  # Multi-table query example
  SELECT u.username, COUNT(*) as uploads
  FROM users u JOIN access_logs a ON u.id = a.user_id
  WHERE a.operation = 'upload';

Interactive mode:
  server-log-analyzer query --db logs.db --table logs

Direct query:
  server-log-analyzer query --db logs.db --sql "SELECT COUNT(*) FROM logs"

Table-specific query:
  server-log-analyzer query --table users --sql "SELECT COUNT(*) FROM users"

Note: This command currently accepts raw SQL queries. In future versions,
this could be extended to support natural language queries that are
automatically translated to SQL using AI/ML models.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runQueryCommand(dbFile, tableName, sqlQuery)
		},
	}

	// Define command flags
	cmd.Flags().StringVarP(&dbFile, "db", "d", config.DefaultDatabaseFile, config.DatabaseFileDescription)
	cmd.Flags().StringVarP(&tableName, "table", "t", config.DefaultTableName, config.TableNameDescription+" (used as context for queries)")
	cmd.Flags().StringVarP(&sqlQuery, "sql", "s", "", "SQL query to execute (if not provided, enters interactive mode)")

	return cmd
}

// runQueryCommand executes the query logic
func runQueryCommand(dbFile, tableName, sqlQuery string) error {
	// Validate database file exists
	if _, err := os.Stat(dbFile); os.IsNotExist(err) {
		return fmt.Errorf("database file does not exist: %s\nPlease run 'load' command first", dbFile)
	}

	// Initialize database connection
	db, err := database.Initialize(dbFile)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Execute single query or enter interactive mode
	if sqlQuery != "" {
		return executeSingleQuery(db, sqlQuery, tableName)
	}

	return enterInteractiveMode(db, dbFile, tableName)
}

// executeSingleQuery runs a single SQL query and displays results
func executeSingleQuery(db database.DB, query string, tableName string) error {
	// Substitute {table} placeholder with actual table name
	query = strings.ReplaceAll(query, "{table}", tableName)

	fmt.Printf("Executing query: %s\n\n", query)

	// Validate that query is read-only
	if err := ValidateReadOnlyQuery(query); err != nil {
		return fmt.Errorf("query validation failed: %w", err)
	}

	results, err := database.ExecuteQuery(db, query)
	if err != nil {
		return fmt.Errorf("query execution failed: %w", err)
	}

	displayResults(results)
	return nil
}

// enterInteractiveMode provides an interactive SQL query interface
func enterInteractiveMode(db database.DB, dbFile string, tableName string) error {
	fmt.Printf("Connected to database: %s\n", dbFile)
	fmt.Printf("Default table context: %s\n", tableName)
	fmt.Println("Interactive SQL query mode. Type 'exit' or 'quit' to exit.")
	fmt.Println("SECURITY: Only read-only queries (SELECT, WITH, EXPLAIN) are allowed.")
	fmt.Println("TIP: Use {table} as a placeholder for the default table name.")
	fmt.Println("Example queries:")
	fmt.Printf("  SELECT COUNT(DISTINCT username) as unique_users FROM %s;\n", tableName)
	fmt.Printf("  SELECT COUNT(*) FROM %s WHERE operation = 'upload' AND size > 50;\n", tableName)
	fmt.Println("  SELECT COUNT(DISTINCT username) as unique_users FROM {table};")
	fmt.Println("  PRAGMA table_info(" + tableName + ");  -- Show table schema")
	fmt.Println("  .tables                              -- List all tables")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("sql> ")

		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())

		// Handle exit commands
		if input == "exit" || input == "quit" {
			fmt.Println("Goodbye!")
			break
		}

		// Skip empty input
		if input == "" {
			continue
		}

		// Handle special commands
		if input == ".tables" {
			if err := showTables(db); err != nil {
				fmt.Printf("Error listing tables: %v\n\n", err)
			}
			continue
		}

		// Substitute {table} placeholder with actual table name
		query := strings.ReplaceAll(input, "{table}", tableName)

		// Execute query
		// Validate that query is read-only
		if err := ValidateReadOnlyQuery(query); err != nil {
			fmt.Printf("Error: %v\n\n", err)
			continue
		}

		results, err := database.ExecuteQuery(db, query)
		if err != nil {
			fmt.Printf("Error: %v\n\n", err)
			continue
		}

		displayResults(results)
		fmt.Println()
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading input: %w", err)
	}

	return nil
}

// displayResults formats and prints query results
func displayResults(results []map[string]interface{}) {
	if len(results) == 0 {
		fmt.Println("No results found.")
		return
	}

	// Get column names from first result
	var columns []string
	for column := range results[0] {
		columns = append(columns, column)
	}

	// Print header
	for i, column := range columns {
		if i > 0 {
			fmt.Print(" | ")
		}
		fmt.Printf("%-15s", column)
	}
	fmt.Println()

	// Print separator
	for i := range columns {
		if i > 0 {
			fmt.Print(" | ")
		}
		fmt.Print(strings.Repeat("-", 15))
	}
	fmt.Println()

	// Print rows
	for _, row := range results {
		for i, column := range columns {
			if i > 0 {
				fmt.Print(" | ")
			}
			fmt.Printf("%-15v", row[column])
		}
		fmt.Println()
	}

	fmt.Printf("\n(%d rows)\n", len(results))
}

// ValidateReadOnlyQuery ensures the SQL query is read-only and safe to execute
// Prevents data modification, schema changes, and other potentially harmful operations
func ValidateReadOnlyQuery(query string) error {
	// Normalize query: trim whitespace and convert to lowercase
	normalizedQuery := strings.TrimSpace(strings.ToLower(query))

	// Remove comments (basic comment removal)
	// Remove single-line comments (-- comment)
	commentRegex := regexp.MustCompile(`--.*`)
	normalizedQuery = commentRegex.ReplaceAllString(normalizedQuery, "")

	// Remove multi-line comments (/* comment */)
	multiCommentRegex := regexp.MustCompile(`/\*.*?\*/`)
	normalizedQuery = multiCommentRegex.ReplaceAllString(normalizedQuery, "")

	// Trim again after comment removal
	normalizedQuery = strings.TrimSpace(normalizedQuery)

	if normalizedQuery == "" {
		return fmt.Errorf("empty query")
	}

	// Define allowed read-only operations
	allowedPrefixes := []string{
		"select",    // SELECT queries
		"with",      // Common Table Expressions (CTEs)
		"explain",   // Query execution plans
	}

	// Check if query starts with an allowed operation
	queryStartsWithAllowed := false
	for _, prefix := range allowedPrefixes {
		if strings.HasPrefix(normalizedQuery, prefix) {
			queryStartsWithAllowed = true
			break
		}
	}

	// Allow specific PRAGMA queries that are read-only
	if strings.HasPrefix(normalizedQuery, "pragma") {
		allowedPragmas := []string{
			"pragma table_info(",
			"pragma index_list(",
			"pragma index_info(",
			"pragma foreign_key_list(",
			"pragma schema_version",
			"pragma user_version",
			"pragma database_list",
			"pragma compile_options",
		}

		pragmaAllowed := false
		for _, allowedPragma := range allowedPragmas {
			if strings.HasPrefix(normalizedQuery, allowedPragma) {
				pragmaAllowed = true
				break
			}
		}

		if !pragmaAllowed {
			return fmt.Errorf("PRAGMA statement not allowed. Only read-only PRAGMA statements are permitted")
		}
		queryStartsWithAllowed = true
	}

	if !queryStartsWithAllowed {
		return fmt.Errorf("only read-only queries are allowed (SELECT, WITH, EXPLAIN, and read-only PRAGMA)")
	}

	// Define forbidden keywords that indicate write operations
	forbiddenKeywords := []string{
		"insert", "update", "delete", "drop", "create", "alter",
		"truncate", "replace", "merge", "upsert",
		"attach", "detach", "vacuum", "reindex",
		"begin", "commit", "rollback", "savepoint",
	}

	// Check for forbidden keywords anywhere in the query
	for _, keyword := range forbiddenKeywords {
		// Use word boundary regex to match whole words only
		keywordRegex := regexp.MustCompile(`\b` + regexp.QuoteMeta(keyword) + `\b`)
		if keywordRegex.MatchString(normalizedQuery) {
			return fmt.Errorf("forbidden keyword '%s' detected. Only read-only operations are allowed", strings.ToUpper(keyword))
		}
	}

	// Additional safety: check for semicolon-separated statements
	statements := strings.Split(normalizedQuery, ";")
	if len(statements) > 2 { // Allow one statement + empty string after final semicolon
		return fmt.Errorf("multiple statements not allowed. Please execute one query at a time")
	}

	// Validate that we don't have nested forbidden operations in subqueries
	if strings.Contains(normalizedQuery, "(") && strings.Contains(normalizedQuery, ")") {
		// Extract content within parentheses and validate recursively
		// This is a simple check - a more sophisticated parser might be needed for complex cases
		for _, keyword := range forbiddenKeywords {
			keywordRegex := regexp.MustCompile(`\b` + regexp.QuoteMeta(keyword) + `\b`)
			if keywordRegex.MatchString(normalizedQuery) {
				return fmt.Errorf("forbidden keyword '%s' detected in subquery. Only read-only operations are allowed", strings.ToUpper(keyword))
			}
		}
	}

	return nil
}

// showTables lists all tables in the database
func showTables(db database.DB) error {
	query := "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
	results, err := database.ExecuteQuery(db, query)
	if err != nil {
		return err
	}

	if len(results) == 0 {
		fmt.Println("No tables found in database.")
		return nil
	}

	fmt.Println("Tables in database:")
	for _, result := range results {
		if tableName, ok := result["name"].(string); ok {
			fmt.Printf("  %s\n", tableName)
		}
	}
	fmt.Println()
	return nil
}
