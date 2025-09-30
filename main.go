package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/joho/godotenv"
	"superalign.ai/config"
	"superalign.ai/internal/parser"
)

func main() {
	// Load configuration
	cfg := config.LoadConfig()

	// Load environment variables from .env file
	if err := godotenv.Load(); err != nil {
		log.Println("Error loading .env file:", err)
	}

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

// processFile determines the file type and routes to the appropriate parser
func processFile(_ *config.Config, inputPath, outputPath string) error {
	// Get file extension
	ext := strings.ToLower(filepath.Ext(inputPath))

	fmt.Printf("Processing file: %s (type: %s)\n", inputPath, ext)

	switch ext {
	case ".log", ".cef":
		// CEF log format - use CEF parser
		fmt.Println("Detected CEF log format, using CEF parser...")
		cefParser := parser.NewCEFParser()
		return cefParser.ParseLogFileToJSON(inputPath, outputPath)

	case ".csv":
		// CSV format - use CSV parser
		fmt.Println("Detected CSV format, using CSV parser...")
		csvParser := parser.NewCSVParser()
		// Auto-convert to Parquet for better performance
		return csvParser.ParseCSVToKafka(inputPath, outputPath, true)

	case ".parquet":
		// Parquet format - use CSV parser (it handles both CSV and Parquet)
		fmt.Println("Detected Parquet format, using CSV parser...")
		csvParser := parser.NewCSVParser()
		return csvParser.ParseCSVToKafka(inputPath, outputPath, true)

	default:
		return fmt.Errorf("unsupported file format: %s (supported: .log, .cef, .csv, .parquet)", ext)
	}
}
