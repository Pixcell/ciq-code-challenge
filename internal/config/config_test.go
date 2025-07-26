package config

import (
	"testing"
)

// TestConfigConstants tests that the configuration constants are properly defined
func TestConfigConstants(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected string
	}{
		{
			name:     "DefaultDatabaseFile should be server_logs.db",
			value:    DefaultDatabaseFile,
			expected: "server_logs.db",
		},
		{
			name:     "DatabaseFileDescription should not be empty",
			value:    DatabaseFileDescription,
			expected: "Path to SQLite database file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.value != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, tt.value)
			}
		})
	}
}

// TestDefaultDatabaseFileNotEmpty ensures DefaultDatabaseFile is not empty
func TestDefaultDatabaseFileNotEmpty(t *testing.T) {
	if DefaultDatabaseFile == "" {
		t.Error("DefaultDatabaseFile should not be empty")
	}
}

// TestDatabaseFileDescriptionNotEmpty ensures DatabaseFileDescription is not empty
func TestDatabaseFileDescriptionNotEmpty(t *testing.T) {
	if DatabaseFileDescription == "" {
		t.Error("DatabaseFileDescription should not be empty")
	}
}
