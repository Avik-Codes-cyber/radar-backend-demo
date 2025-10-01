package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/joho/godotenv"
	"superalign.ai/config"
	"superalign.ai/internal/parser"
	"superalign.ai/internal/server"
)

func main() {
	// Load environment variables from .env file
	if err := godotenv.Load(); err != nil {
		log.Println("Error loading .env file:", err)
	}

	// Load configuration
	cfg := config.LoadConfig()

	// Check if server mode is enabled
	if cfg.SyslogServerEnabled {
		// Run in server mode (real-time log ingestion)
		fmt.Println("=== Starting in SERVER MODE ===")
		fmt.Printf("Mode: Real-time log ingestion from Fortigate\n")
		fmt.Printf("Protocol: %s, Port: %d\n", cfg.SyslogServerProtocol, cfg.SyslogServerPort)

		if err := runServerMode(cfg); err != nil {
			log.Fatalf("Server mode failed: %v", err)
		}
	} else {
		// Run in file processing mode (batch processing)
		fmt.Println("=== Starting in FILE PROCESSING MODE ===")

		inputPath := os.Getenv("INPUT_FILE_PATH")
		outputPath := os.Getenv("OUTPUT_FILE_PATH")

		// Ensure input file exists
		if _, err := os.Stat(inputPath); os.IsNotExist(err) {
			log.Fatalf("Input file does not exist: %s", inputPath)
		}

		// Determine file type and route to appropriate parser
		if err := processFile(cfg, inputPath, outputPath); err != nil {
			log.Fatalf("Failed to process file: %v", err)
		}

		fmt.Println("Processing completed successfully")
	}
}

// runServerMode starts the syslog server for real-time log ingestion
func runServerMode(cfg *config.Config) error {
	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Create and start syslog server
	syslogServer := server.NewSyslogServer(cfg, cfg.SyslogServerProtocol, cfg.SyslogServerPort)

	// Start server in a goroutine
	errChan := make(chan error, 1)
	go func() {
		if err := syslogServer.Start(ctx); err != nil {
			errChan <- err
		}
	}()

	fmt.Println("Syslog server is running. Press Ctrl+C to stop.")
	fmt.Println("")
	fmt.Println("To configure Fortigate to send logs here:")
	fmt.Printf("1. Log into your Fortigate device\n")
	fmt.Printf("2. Go to Log & Report > Log Settings\n")
	fmt.Printf("3. Configure remote logging:\n")
	fmt.Printf("   - IP Address: <this server's IP>\n")
	fmt.Printf("   - Port: %d\n", cfg.SyslogServerPort)
	fmt.Printf("   - Protocol: %s\n", strings.ToUpper(cfg.SyslogServerProtocol))
	fmt.Println("")

	// Wait for shutdown signal or error
	select {
	case <-sigChan:
		fmt.Println("\nReceived shutdown signal, stopping server...")
		cancel()
		syslogServer.Stop()
		fmt.Println("Server stopped gracefully")
		return nil
	case err := <-errChan:
		cancel()
		syslogServer.Stop()
		return fmt.Errorf("server error: %w", err)
	}
}

// processFile determines the file type and routes to the appropriate parser
func processFile(_ *config.Config, inputPath, outputPath string) error {
	// Get file extension
	ext := strings.ToLower(filepath.Ext(inputPath))
	filename := strings.ToLower(filepath.Base(inputPath))

	fmt.Printf("Processing file: %s (type: %s)\n", inputPath, ext)

	// Detect format by extension and filename patterns
	switch {
	case ext == ".cef":
		// CEF log format - use CEF parser
		fmt.Println("Detected CEF log format, using CEF parser...")
		cefParser := parser.NewCEFParser()
		return cefParser.ParseLogFileToJSON(inputPath, outputPath)

	case ext == ".syslog" || strings.Contains(filename, "syslog"):
		// Syslog format - use Syslog parser (RFC5424 by default)
		fmt.Println("Detected syslog format, using Syslog parser (RFC5424)...")
		syslogParser := parser.NewSyslogParser(5424)
		return syslogParser.ParseLogFileToKafka(inputPath, outputPath)

	case ext == ".log":
		// .log can be CEF, Syslog, or generic log - try to detect from content
		fmt.Println("Detected .log file, attempting format detection...")
		format, err := detectLogFormat(inputPath)
		if err != nil {
			return fmt.Errorf("failed to detect log format: %w", err)
		}

		switch format {
		case "CEF":
			fmt.Println("Detected CEF format, using CEF parser...")
			cefParser := parser.NewCEFParser()
			return cefParser.ParseLogFileToJSON(inputPath, outputPath)
		case "RFC5424":
			fmt.Println("Detected RFC5424 syslog format, using Syslog parser...")
			syslogParser := parser.NewSyslogParser(5424)
			return syslogParser.ParseLogFileToKafka(inputPath, outputPath)
		case "RFC3164":
			fmt.Println("Detected RFC3164 syslog format, using Syslog parser...")
			syslogParser := parser.NewSyslogParser(3164)
			return syslogParser.ParseLogFileToKafka(inputPath, outputPath)
		default:
			return fmt.Errorf("unknown log format in file")
		}

	case ext == ".csv":
		// CSV format - use CSV parser
		fmt.Println("Detected CSV format, using CSV parser...")
		csvParser := parser.NewCSVParser()
		// Auto-convert to Parquet for better performance
		return csvParser.ParseCSVToKafka(inputPath, outputPath, true)

	case ext == ".parquet":
		// Parquet format - use CSV parser (it handles both CSV and Parquet)
		fmt.Println("Detected Parquet format, using CSV parser...")
		csvParser := parser.NewCSVParser()
		return csvParser.ParseCSVToKafka(inputPath, outputPath, true)

	default:
		return fmt.Errorf("unsupported file format: %s (supported: .cef, .syslog, .log, .csv, .parquet)", ext)
	}
}

// detectLogFormat attempts to detect the format of a .log file by examining its content
func detectLogFormat(inputPath string) (string, error) {
	file, err := os.Open(inputPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// Check first few lines
	for i := 0; i < 10 && scanner.Scan(); i++ {
		line := scanner.Text()

		// Check for CEF format
		if strings.Contains(line, "CEF:") {
			return "CEF", nil
		}

		// Check for RFC5424 format (has version number after priority)
		if len(line) > 0 && line[0] == '<' {
			// Try to find priority and version
			if idx := strings.Index(line, ">"); idx > 0 && idx < 10 {
				rest := line[idx+1:]
				// RFC5424 has version number (1-999) followed by space
				if len(rest) > 0 && rest[0] >= '0' && rest[0] <= '9' {
					return "RFC5424", nil
				}
				// RFC3164 has timestamp directly after priority
				return "RFC3164", nil
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return "", err
	}

	return "", fmt.Errorf("unable to detect log format")
}
