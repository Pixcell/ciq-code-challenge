#!/bin/bash

# Build script for server-log-analyzer
echo "Building server-log-analyzer..."

# Build the main binary
go build -o bin/server-log-analyzer ./cmd/

if [ $? -eq 0 ]; then
    echo "Build successful! Binary created at: bin/server-log-analyzer"
    echo ""
    echo "Usage examples:"
    echo "  ./bin/server-log-analyzer load --file server_log.csv"
    echo "  ./bin/server-log-analyzer query --sql \"SELECT COUNT(*) FROM logs\""
    echo "  ./bin/server-log-analyzer query  # Interactive mode"
else
    echo "Build failed!"
    exit 1
fi
