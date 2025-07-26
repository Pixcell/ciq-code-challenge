// Package models defines the data structures used throughout the application
package models

import (
	"fmt"
	"time"
)

// LogEntry represents a single log entry from the server log file
// This structure maps directly to the CSV columns: timestamp, username, operation, size
type LogEntry struct {
	ID        int64     `db:"id" json:"id"`                // Auto-increment primary key
	Timestamp time.Time `db:"timestamp" json:"timestamp"`  // UNIX timestamp converted to time.Time
	Username  string    `db:"username" json:"username"`    // Unique user identifier
	Operation string    `db:"operation" json:"operation"`  // Either "upload" or "download"
	Size      int       `db:"size" json:"size"`            // File size in kB
}

// String returns a human-readable representation of the log entry
func (l LogEntry) String() string {
	return fmt.Sprintf("%s: %s %s %dkB",
		l.Timestamp.Format("2006-01-02 15:04:05"),
		l.Username,
		l.Operation,
		l.Size)
}
