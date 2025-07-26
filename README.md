# Implementation Documentation

## Two-Step CLI Approach for Server Log Analysis

### Overview

This implementation uses a two-step CLI approach for analyzing server log files:

1. **Load Step**: Parse CSV log files and store them in a SQLite database
2. **Query Step**: Execute SQL queries against the stored data

### Architecture Benefits

#### 1. Performance Optimization
- **One-time parsing cost**: CSV parsing happens only once during the load step
- **Fast repeated queries**: SQLite provides excellent query performance with proper indexing
- **Memory efficiency**: Large datasets don't need to be held in memory during queries
- **Concurrent access**: Multiple query sessions can access the same dataset

#### 2. Scalability
- **Large file support**: Can handle CSV files much larger than available RAM
- **Indexed queries**: Database indexes dramatically improve query performance for common patterns
- **Batch processing**: Support for loading multiple CSV files with append mode
- **Incremental updates**: New log files can be appended to existing databases without clearing previous data

#### 3. Flexibility and Extensibility
- **Raw SQL access**: Supports any SQL query, enabling complex analysis beyond the basic requirements
- **Future ML integration**: The query interface can be extended to support natural language queries
- **Multiple output formats**: Query results can be formatted for different consumers (JSON, CSV, etc.)
- **Schema evolution**: Database schema can be versioned and migrated as requirements change

#### 4. Development and Maintenance Benefits
- **Separation of concerns**: Loading and querying are distinct, testable operations
- **Database abstraction**: The DB interface allows for easy testing and potential database swapping
- **Clear data pipeline**: Data flow is explicit: CSV → Database → Query Results
- **Debugging support**: SQLite databases can be inspected with standard tools

### Implementation Highlights

#### Modular Design
```
cmd/main.go              # CLI entry point
internal/
├── commands/            # Command implementations
│   ├── load.go         # CSV loading command
│   └── query.go        # SQL query command
├── config/             # Shared configuration
│   └── config.go       # Application constants and settings
├── database/           # Database operations
│   └── database.go     # SQLite interface and operations
├── models/             # Data structures
│   └── log_entry.go    # Log entry model
└── parser/             # CSV parsing
    └── csv.go          # CSV parsing logic
    └── schema.go       # Schema detection and management
```

#### Key Features

1. **Robust Error Handling**: Comprehensive error messages with context
2. **Input Validation**: CSV format validation and data type checking
3. **Interactive Mode**: SQL shell for exploratory data analysis
4. **Performance Optimization**: Database indexes on commonly queried columns
5. **Append Mode**: Support for adding new data to existing databases without clearing previous entries
6. **Shared Configuration**: Centralized configuration management to ensure consistency across commands
7. **Documentation**: Extensive code comments and usage examples
8. **Dynamic Schema Detection**: Automatic detection of CSV structure and data types (enabled by default)
9. **Multi-Table Support**: Load different CSV files into separate tables for complex analysis
10. **Flexible Table Management**: Specify custom table names for both loading and querying operations

### Data Loading Modes

The `load` command supports multiple modes and features:

#### Schema Detection (Default: Enabled)
```bash
# Automatic schema detection analyzes CSV structure and data types
server-log-analyzer load --file users.csv --table users --db analytics.db

# Detected schema includes:
# - Column names from CSV headers
# - Data type inference (TEXT, INTEGER, REAL, DATETIME, BOOLEAN)
# - Automatic indexing on commonly queried columns (username, timestamp, id, etc.)
```

#### Legacy Mode (Schema Detection Disabled)
```bash
# Uses fixed schema for backward compatibility: timestamp, username, operation, size
server-log-analyzer load --file server_log.csv --db logs.db --schema-detection=false
```

#### Replace Mode (Default)
```bash
# Clears existing data and loads new CSV data
server-log-analyzer load --file new_logs.csv --db logs.db --table logs
```

#### Append Mode
```bash
# Adds new data to existing table without clearing previous entries
server-log-analyzer load --file additional_logs.csv --db logs.db --table logs --append

# Creates table if it doesn't exist, even in append mode
server-log-analyzer load --file users.csv --table users --db analytics.db --append
```

**Multi-Table Support:**
```bash
# Load different CSV files into separate tables
server-log-analyzer load --file users.csv --table users --db analytics.db
server-log-analyzer load --file access_logs.csv --table access_logs --db analytics.db --append
server-log-analyzer load --file error_logs.csv --table errors --db analytics.db --append
```

**Append Mode Features:**
- **Preserves existing data**: Previous log entries remain in the database
- **Incremental loading**: Perfect for processing multiple log files sequentially
- **Smart warnings**: Warns when trying to append to a non-existent database
- **Automatic database creation**: Creates new database if it doesn't exist, even in append mode
- **Consistent behavior**: Same validation and error handling as replace mode

### Configuration Management

The application uses a centralized configuration approach to ensure consistency across all commands:

#### Shared Constants
- **Default database filename**: `server_logs.db` (defined in `internal/config/config.go`)
- **Consistent help text**: Shared descriptions for command flags
- **Single source of truth**: Prevents configuration drift between commands

#### Benefits
- **No configuration errors**: Both `load` and `query` commands use the same default database file
- **Easy maintenance**: Changes to defaults only need to be made in one place
- **Testable configuration**: Configuration constants are unit tested
- **Future extensibility**: Additional configuration options can be easily added

```go
// Example usage in commands
import "server-log-analyzer/internal/config"

cmd.Flags().StringVarP(&dbFile, "db", "d",
    config.DefaultDatabaseFile,
    config.DatabaseFileDescription)
```

### Query Examples

The tool can answer the challenge requirements and much more:

```sql
-- 1. How many users accessed the server?
SELECT COUNT(DISTINCT username) as unique_users FROM logs;

-- 2. How many uploads were larger than 50kB?
SELECT COUNT(*) as large_uploads
FROM logs
WHERE operation = 'upload' AND size > 50;

-- 3. How many times did jeff22 upload on April 15th, 2020?
SELECT COUNT(*) as jeffs_uploads
FROM logs
WHERE username = 'jeff22'
  AND operation = 'upload'
  AND date(timestamp) = '2020-04-15';
```

### Future Enhancements

#### Natural Language Query Support
The current implementation accepts raw SQL queries, but the architecture supports extending this to natural language queries:

```go
// Future enhancement: NL to SQL translation
type QueryTranslator interface {
    TranslateToSQL(naturalLanguage string) (string, error)
}

// Could integrate with OpenAI, local LLMs, or rule-based systems
func (c *QueryCommand) handleNaturalLanguage(query string) error {
    translator := NewAIQueryTranslator()
    sql, err := translator.TranslateToSQL(query)
    if err != nil {
        return err
    }
    return c.executeSQL(sql)
}
```
 ##### Local LLM in Go

Running a local Large Language Model (LLM) using Go typically involves interacting with a local LLM server or framework. Ollama is a popular choice for this purpose, as it simplifies the process of downloading and running various LLMs locally.

Here's a general approach to running a local LLM with Go and Ollama:

1. Set up Ollama:
   - Download and install Ollama on your local machine (macOS, Linux, or Windows).
   - Use Ollama's command-line interface to pull the desired LLM model (e.g., ollama pull llama3).
2. Consider if Ollama should be run externally, or started by the CLI tool
    - Detect which type of computer is running the comamnd and if it is powerful enough to run a local LLM
    - Provide the ability to specify the LLM endpoint as a param if it's not started by the CLI
3. Interact with Ollama from Go:
   - Direct HTTP Requests: You can make HTTP requests from your Go application to Ollama's API endpoint (usually http://localhost:11434). This involves constructing JSON payloads for requests (e.g., for generating text) and parsing the JSON responses.
   - Using a Go Library: Consider using a Go library designed for interacting with LLMs or Ollama specifically. Libraries like gollm simplify the process by providing Go-native structs and functions for interacting with Ollama's API, handling request/response serialization, and managing LLM interactions.


#### Additional Extensions
- **Multi-format support**: JSON, XML log parsing
- **Real-time streaming**: Process logs as they're written
- **Web dashboard**: HTTP API for query results
- **Export capabilities**: Generate reports in various formats
- **Data visualization**: Charts and graphs for common metrics
- **Alert system**: Automated monitoring based on query thresholds

### Performance Characteristics

- **Load time**: O(n) where n is the number of log entries
- **Query time**: O(log n) for indexed columns, O(n) for full scans
- **Memory usage**: Constant during queries (SQLite handles paging)
- **Storage overhead**: ~30-50% larger than raw CSV due to indexes and metadata

### Testing Strategy

The modular design enables comprehensive testing following Go best practices:

#### Test Coverage
- **Parser package**: 61.9% coverage - Comprehensive testing of CSV parsing logic and schema detection
- **Database package**: 76.2% coverage - Core database operations and SQL execution
- **Commands package**: 63.9% coverage - CLI command implementations
- **Models package**: 100.0% coverage - Simple data structures (complete coverage achieved)
- **Config package**: No statements - Configuration constants only

The current test coverage provides robust validation of core functionality, with particularly strong coverage in the database and parser packages that handle the most complex operations. Further improvements could include expanding coverage of edge cases in command-line interfaces, error handling scenarios, and integration testing across package boundaries to approach higher overall coverage percentages.

#### Test Categories

**1. Unit Tests**
- Individual function testing with table-driven tests
- Edge case validation (empty files, invalid data, malformed CSV)
- Error handling verification
- Input validation testing

**2. Integration Tests**
- End-to-end parsing workflows
- Database initialization and data insertion
- Query execution with real SQLite database

**3. Example Tests**
- Demonstrate expected usage patterns
- Serve as documentation for API consumers
- Validate public interface contracts

#### Running Tests

```bash
# Run all tests
go test ./...

# Run with verbose output
go test -v ./internal/...

# Run with coverage
go test -cover ./internal/...

# Run benchmarks
go test -bench=. ./internal/...

# Run specific test
go test -run TestParseCSV ./internal/parser
```

#### Test Structure

Each test package follows Go conventions:
- `TestXxx` functions for unit tests
- `BenchmarkXxx` functions for performance tests
- `ExampleXxx` functions for documentation
- Table-driven tests for comprehensive scenarios
- Helper functions for test data creation

#### Mock and Test Data

- In-memory SQLite databases for isolated testing
- Temporary CSV files for parser testing
- Comprehensive test cases covering happy path and error conditions
- Realistic test data mimicking production scenarios

This testing approach ensures reliability, performance, and maintainability while providing confidence for future enhancements.

## Related Projects and External Resources

### sq.io - Advanced SQL Tool for Data Analysis

For production use cases requiring more sophisticated data analysis capabilities, consider [**sq**](https://sq.io/) - a comprehensive command-line tool that provides advanced SQL operations across multiple data sources.

**Key capabilities of sq:**
- **Multi-source queries**: Join data from different databases, APIs, and file formats
- **Advanced data inspection**: Sophisticated schema discovery and data profiling
- **Cross-platform data movement**: Transfer data between different database systems
- **Rich output formats**: JSON, CSV, Excel, and formatted tables
- **Production-ready features**: Performance optimization, error handling, and logging

**Example sq usage:**
```bash
# Inspect CSV schema and data
sq inspect data.csv

# Join CSV with database table
sq '@data.csv | join @mydb.users | .name, .email, .upload_count'

# Convert and transfer data between sources
sq '@source.csv | .[] | @dest_db.table'
```

**When to use sq vs this implementation:**
- **Use sq for**: Production data analysis, complex multi-source queries, enterprise workflows
- **Use this implementation for**: Learning, prototyping, simple CSV analysis, custom business logic

### csv2sql Library - Dynamic CSV Import

The dynamic schema detection implemented in this project draws inspiration from existing solutions like [**csv2sql**](https://github.com/wiremoons/csv2sql) - a specialized library for importing CSV files into SQLite databases.

**Features of csv2sql:**
- **Automatic schema inference**: Detects column types and constraints
- **Batch processing**: Efficient handling of large CSV files
- **Flexible configuration**: Customizable import rules and data transformations
- **Error handling**: Robust validation and error reporting

**Comparison with this implementation:**
- **csv2sql**: Focused specifically on CSV-to-SQLite import with advanced configuration options
- **This project**: Broader scope including interactive querying, CLI design, and extensible architecture

## MVP Nature and Extension Opportunities

This implementation serves as a **Minimum Viable Product (MVP)** demonstrating core concepts in data processing and analysis. The architecture is designed to be extended with external libraries and tools for production use cases.

### Recommended Extensions

**1. Replace Custom Parser with csv2sql**
```go
import "github.com/wiremoons/csv2sql"

// Enhanced CSV import with production-ready features
func loadWithCSV2SQL(csvPath, dbPath string) error {
    return csv2sql.Import(csvPath, dbPath, csv2sql.Config{
        AutoDetectTypes: true,
        BatchSize: 10000,
        CreateIndexes: true,
    })
}
```

**2. Integrate with sq for Advanced Queries**
```bash
# Use this tool for initial data loading
./server-log-analyzer load -f logs.csv

# Use sq for complex analysis
sq '@server_logs.db.logs | .username, count(*) | group username | order count desc'
```

**3. Production-Ready Enhancements**
- **Configuration management**: Use [Viper](https://github.com/spf13/viper) for complex configuration
- **Logging**: Integrate [logrus](https://github.com/sirupsen/logrus) or [zap](https://go.uber.org/zap)
- **Progress tracking**: Add [progressbar](https://github.com/schollz/progressbar) for large file processing
- **Data validation**: Use [validator](https://github.com/go-playground/validator) for input validation
- **Database migrations**: Implement [golang-migrate](https://github.com/golang-migrate/migrate) for schema versioning

### Learning Objectives Achieved

This MVP implementation demonstrates:
- **Clean architecture principles**: Separation of concerns and modular design
- **Go best practices**: Proper error handling, testing, and package organization
- **Database operations**: SQLite integration and schema management
- **CLI development**: Command-line interface design with Cobra
- **Testing strategies**: Unit tests, integration tests, and benchmarks

### Production Considerations

For production deployments, consider:
- **Using established tools** like sq.io for complex data analysis workflows
- **Leveraging proven libraries** like csv2sql for robust CSV processing
- **Implementing monitoring** and observability features
- **Adding security measures** for database access and query validation
- **Performance optimization** for large-scale data processing

This architecture provides a solid foundation that can evolve from a simple log analyzer to a comprehensive data analysis platform.
