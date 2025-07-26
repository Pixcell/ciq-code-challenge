// Package main provides the CLI entry point for the server log analyzer
// This tool provides two main commands:
// 1. load - Parse CSV log files and store them in SQLite database
// 2. query - Execute SQL queries against the stored log data
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"server-log-analyzer/internal/commands"
)

func main() {
	// Root command defines the base command when called without any subcommands
	var rootCmd = &cobra.Command{
		Use:   "server-log-analyzer",
		Short: "A CLI tool for analyzing server log files",
		Long: `Server Log Analyzer is a two-step CLI tool for processing and querying server log data.

Step 1: Load CSV log files into a SQLite database for efficient querying
Step 2: Execute SQL queries against the stored data to generate metrics

This approach provides excellent performance for repeated queries and supports
complex analysis scenarios.`,
	}

	// Add subcommands
	rootCmd.AddCommand(commands.NewLoadCommand())
	rootCmd.AddCommand(commands.NewQueryCommand())

	// Execute the root command
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error executing command: %v\n", err)
		os.Exit(1)
	}
}
