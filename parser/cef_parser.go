package parser

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/ren3gadem4rm0t/cef-parser-go/parser"
	"superalign.ai/models"
)

// CEFParser wraps parsing logic
type CEFParser struct{}

// ParseLogFileToJSON parses all CEF logs from a file and writes them into a JSON file
func (p *CEFParser) ParseLogFileToJSON(inputPath, outputPath string) error {
	file, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	safeParser := parser.NewParser(parser.SafeConfig())

	var results []*models.CEFLogEntry

	err = safeParser.ParseStream(file, func(cef *parser.CEF, parseErr error) bool {
		if parseErr != nil {
			// stop if bad log line
			err = fmt.Errorf("failed to parse log: %w", parseErr)
			return false
		}

		severity, _ := strconv.Atoi(cef.Severity)

		// Get extensions as JSON and parse to map
		extMap := make(map[string]string)
		if cef.Extensions != nil {
			extJSON := cef.Extensions.AsJSON()
			json.Unmarshal([]byte(extJSON), &extMap)
		}

		entry := &models.CEFLogEntry{
			DeviceVendor:  cef.DeviceVendor,
			DeviceProduct: cef.DeviceProduct,
			EventID:       cef.SignatureID,
			Severity:      severity,
			Src:           extMap["src"],
			Dst:           extMap["dst"],
			Name:          cef.Name,
		}
		results = append(results, entry)

		return true // keep parsing
	})

	if err != nil {
		return err
	}

	// Marshal to JSON
	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal logs to JSON: %w", err)
	}

	// Write JSON to file
	if writeErr := os.WriteFile(outputPath, data, 0644); writeErr != nil {
		return fmt.Errorf("failed to write JSON file: %w", writeErr)
	}

	return nil
}
