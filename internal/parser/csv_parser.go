package parser

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"superalign.ai/config"
	"superalign.ai/internal/kafka"
	"superalign.ai/models"
)

type CSVParser struct {
	metrics *ParserMetrics
}

type csvParseJob struct {
	rowNum    int
	row       []string
	headerMap map[string]int
}

func NewCSVParser() *CSVParser {
	return &CSVParser{
		metrics: &ParserMetrics{},
	}
}

// ParseCSVToParquet converts CSV to Parquet using DuckDB for efficient processing
func (p *CSVParser) ParseCSVToParquet(inputPath, parquetPath string) error {
	// Open DuckDB connection
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer db.Close()

	// Get absolute paths
	absInputPath, err := filepath.Abs(inputPath)
	if err != nil {
		return fmt.Errorf("failed to get absolute input path: %w", err)
	}
	absParquetPath, err := filepath.Abs(parquetPath)
	if err != nil {
		return fmt.Errorf("failed to get absolute parquet path: %w", err)
	}

	// Use DuckDB to convert CSV to Parquet
	// DuckDB auto-detects CSV format and handles all the parsing
	query := fmt.Sprintf(`
		COPY (
			SELECT * FROM read_csv('%s', 
				header=true,
				auto_detect=true,
				ignore_errors=true,
				quote='"',
				escape='"',
				delim=','
			)
		) TO '%s' (FORMAT PARQUET, COMPRESSION SNAPPY);
	`, absInputPath, absParquetPath)

	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("failed to convert CSV to Parquet: %w", err)
	}

	// Get row count for reporting
	countQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM read_parquet('%s')
	`, absParquetPath)

	var rowCount int64
	if err := db.QueryRow(countQuery).Scan(&rowCount); err == nil {
		fmt.Printf("Converted %d rows to Parquet format using DuckDB\n", rowCount)
	}

	return nil
}

// ParseCSVToKafka processes CSV (or Parquet) and sends to Kafka
func (p *CSVParser) ParseCSVToKafka(inputPath, dlqPath string, useParquet bool) error {
	cfg := config.LoadConfig()
	producer := kafka.NewProducerFromConfig(cfg)
	defer producer.Close()

	dlq, err := os.Create(dlqPath)
	if err != nil {
		return fmt.Errorf("failed to create DLQ file: %w", err)
	}
	defer dlq.Close()

	dlqWriter := bufio.NewWriter(dlq)
	defer dlqWriter.Flush()

	var dlqMutex sync.Mutex

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// If requested to use Parquet but the input is a CSV, convert automatically using DuckDB
	if useParquet {
		ext := filepath.Ext(inputPath)
		if ext == ".csv" || ext == ".CSV" {
			parquetTempPath := inputPath + ".parquet"
			fmt.Printf("Auto-converting CSV to Parquet using DuckDB: %s -> %s\n", inputPath, parquetTempPath)
			if err := p.ParseCSVToParquet(inputPath, parquetTempPath); err != nil {
				return fmt.Errorf("failed to convert CSV to Parquet: %w", err)
			}
			// Use the generated parquet for downstream processing and clean up after
			inputPath = parquetTempPath
			defer os.Remove(parquetTempPath)
		}
	}

	// Determine optimal worker counts from config
	numWorkers := cfg.ParserWorkers
	if numWorkers == 0 {
		numWorkers = runtime.NumCPU()
		if numWorkers < 2 {
			numWorkers = 2
		}
	}

	numPublishers := cfg.PublisherWorkers
	if numPublishers == 0 {
		numPublishers = runtime.NumCPU() / 2
		if numPublishers < 3 {
			numPublishers = 3
		}
	}

	// Use configurable buffer sizes for backpressure
	bufferSize := cfg.ChannelBufferSize
	rawRows := make(chan csvParseJob, bufferSize)
	entries := make(chan *models.CSVLogEntry, bufferSize)

	// Separate wait groups for parsers and publishers to avoid deadlock
	var parserWg sync.WaitGroup
	var publisherWg sync.WaitGroup
	errChan := make(chan error, numWorkers+numPublishers+1)

	// Stage 1: Reader - either CSV or Parquet
	parserWg.Add(1)
	if useParquet {
		// Use DuckDB for dynamic Parquet reading instead of xitongsys
		go p.duckDBParquetReader(ctx, &parserWg, inputPath, rawRows, errChan)
	} else {
		go p.csvReader(ctx, &parserWg, inputPath, rawRows, errChan)
	}

	// Stage 2: Parser workers
	for i := 0; i < numWorkers; i++ {
		parserWg.Add(1)
		go func(workerID int) {
			defer parserWg.Done()

			for job := range rawRows {
				select {
				case <-ctx.Done():
					return
				default:
				}

				p.metrics.linesProcessed.Add(1)

				if len(job.row) == 0 {
					p.metrics.linesFailed.Add(1)
					continue
				}

				entry := p.mapRowToEntry(job.row, job.headerMap)

				select {
				case entries <- entry:
				case <-ctx.Done():
					return
				}
			}
		}(i)
	}

	// Stage 3: Publisher workers (scaled based on config)
	fmt.Printf("Starting pipeline: %d parser workers, %d publisher workers\n", numWorkers, numPublishers)
	for i := 0; i < numPublishers; i++ {
		publisherWg.Add(1)
		go p.publishWorker(ctx, &publisherWg, producer, cfg, entries, dlqWriter, &dlqMutex)
	}

	// Close entries channel after all parsers finish
	go func() {
		parserWg.Wait()
		fmt.Println("All parsers completed, closing entries channel...")
		close(entries)
	}()

	// Wait for parsers and publishers with timeout
	done := make(chan struct{})
	go func() {
		parserWg.Wait()
		publisherWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		fmt.Println("All workers completed successfully")
	case err := <-errChan:
		cancel()
		<-done
		return err
	case <-time.After(time.Duration(cfg.ProcessingTimeoutMin) * time.Minute):
		cancel()
		<-done
		return fmt.Errorf("parsing timeout after %d minutes", cfg.ProcessingTimeoutMin)
	}

	p.metrics.Report()
	return nil
}

// duckDBParquetReader reads Parquet files dynamically using DuckDB
func (p *CSVParser) duckDBParquetReader(ctx context.Context, wg *sync.WaitGroup, inputPath string, rawRows chan csvParseJob, errChan chan error) {
	defer wg.Done()
	defer close(rawRows)

	absPath, err := filepath.Abs(inputPath)
	if err != nil {
		select {
		case errChan <- fmt.Errorf("failed to get absolute path: %w", err):
		default:
		}
		return
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		select {
		case errChan <- fmt.Errorf("failed to open DuckDB: %w", err):
		default:
		}
		return
	}
	defer db.Close()

	// Query all rows from Parquet
	query := fmt.Sprintf("SELECT * FROM read_parquet('%s')", absPath)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		select {
		case errChan <- fmt.Errorf("failed to query Parquet: %w", err):
		default:
		}
		return
	}
	defer rows.Close()

	// Get column names (headers)
	columns, err := rows.Columns()
	if err != nil {
		select {
		case errChan <- fmt.Errorf("failed to get columns: %w", err):
		default:
		}
		return
	}

	// Validate and build header map
	fmt.Printf("Reading Parquet with DuckDB: %d columns, dynamic schema\n", len(columns))
	validateHeaderMapping(columns, reflect.TypeOf(models.CSVLogEntry{}))

	headerMap := buildHeaderMapFromRow(columns)

	// Prepare value holders
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	rowNum := 0
	for rows.Next() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			continue
		}

		rowNum++
		row := make([]string, len(columns))
		for i, val := range values {
			if val != nil {
				row[i] = fmt.Sprintf("%v", val)
			}
		}

		select {
		case <-ctx.Done():
			return
		case rawRows <- csvParseJob{rowNum: rowNum, row: row, headerMap: headerMap}:
		}
	}

	if err := rows.Err(); err != nil {
		select {
		case errChan <- fmt.Errorf("row iteration error: %w", err):
		default:
		}
	}

	fmt.Printf("DuckDB Parquet reader completed: %d rows read\n", rowNum)
}

func (p *CSVParser) csvReader(ctx context.Context, wg *sync.WaitGroup, inputPath string, rawRows chan csvParseJob, errChan chan error) {
	defer wg.Done()
	defer close(rawRows)

	file, err := os.Open(inputPath)
	if err != nil {
		select {
		case errChan <- fmt.Errorf("failed to open CSV: %w", err):
		default:
		}
		return
	}
	defer file.Close()

	csvReader := csv.NewReader(bufio.NewReaderSize(file, 256*1024))
	csvReader.LazyQuotes = true
	csvReader.TrimLeadingSpace = true
	csvReader.ReuseRecord = false // Don't reuse for concurrent processing

	// Read header and build header map (case-insensitive)
	headerRow, err := csvReader.Read()
	if err != nil {
		select {
		case errChan <- fmt.Errorf("failed to read header: %w", err):
		default:
		}
		return
	}

	// Validate header mapping (one-time debug output)
	validateHeaderMapping(headerRow, reflect.TypeOf(models.CSVLogEntry{}))

	headerMap := buildHeaderMapFromRow(headerRow)

	rowNum := 0
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		rowNum++
		select {
		case <-ctx.Done():
			return
		case rawRows <- csvParseJob{rowNum: rowNum, row: append([]string(nil), row...), headerMap: headerMap}:
		}
	}
}

// parquetReaderBatched reads Parquet files in batches for better performance

func (p *CSVParser) publishWorker(ctx context.Context, wg *sync.WaitGroup, producer *kafka.Producer,
	cfg *config.Config, entries chan *models.CSVLogEntry, dlqWriter *bufio.Writer, dlqMutex *sync.Mutex) {
	defer wg.Done()

	const (
		flushInterval = 500 * time.Millisecond
		maxRetries    = 3
		retryBackoff  = 250 * time.Millisecond
	)

	// Use configurable batch size
	maxBatchSize := cfg.KafkaBatchSize

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	batch := make([][]byte, 0, maxBatchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}

		batchSize := len(batch)
		for attempt := 0; attempt < maxRetries; attempt++ {
			publishCtx, publishCancel := context.WithTimeout(ctx, 10*time.Second)
			pubErr := producer.PublishJSONBatch(publishCtx, cfg.KafkaTopic, batch)
			publishCancel()

			if pubErr == nil {
				p.metrics.messagesPublished.Add(uint64(batchSize))
				totalPublished := p.metrics.messagesPublished.Load()
				fmt.Printf("Published batch of %d messages to Kafka topic '%s' (total: %d)\n", batchSize, cfg.KafkaTopic, totalPublished)
				batch = batch[:0]
				return
			}

			fmt.Printf("Kafka publish attempt %d failed: %v\n", attempt+1, pubErr)
			if attempt < maxRetries-1 {
				time.Sleep(retryBackoff * time.Duration(attempt+1))
			}
		}

		// All retries failed - write to DLQ
		fmt.Printf("Batch of %d messages failed after %d retries, writing to DLQ\n", batchSize, maxRetries)
		p.metrics.messagesFailed.Add(uint64(batchSize))
		dlqMutex.Lock()
		for _, b := range batch {
			dlqWriter.Write(b)
			dlqWriter.WriteString("\n")
		}
		dlqMutex.Unlock()
		batch = batch[:0]
	}

	for {
		select {
		case e, ok := <-entries:
			if !ok {
				flush()
				fmt.Println("Publisher worker finished")
				return
			}

			b, err := json.Marshal(e)
			if err != nil {
				p.metrics.messagesFailed.Add(1)
				continue
			}

			batch = append(batch, b)
			if len(batch) >= maxBatchSize {
				flush()
			}

		case <-ticker.C:
			flush()

		case <-ctx.Done():
			flush()
			return
		}
	}
}

// normalizeHeader converts a string to lowercase and removes all non-alphanumeric characters
func normalizeHeader(s string) string {
	s = strings.ToLower(s)   // lowercase
	s = strings.TrimSpace(s) // trim leading/trailing spaces
	// remove all non-alphanumeric characters
	re := regexp.MustCompile(`[^a-z0-9]`)
	s = re.ReplaceAllString(s, "")
	return s
}

// buildHeaderMapFromRow builds a normalized header map from CSV/Parquet header row
func buildHeaderMapFromRow(headerRow []string) map[string]int {
	headerMap := make(map[string]int, len(headerRow))
	for idx, h := range headerRow {
		normalized := normalizeHeader(h)
		headerMap[normalized] = idx
		// Debug: show header normalization
		if h != normalized {
			fmt.Printf("Header mapping: '%s' → '%s' (index %d)\n", h, normalized, idx)
		}
	}
	return headerMap
}

// mapRowToEntry maps a CSV/Parquet row to a CSVLogEntry struct
func (p *CSVParser) mapRowToEntry(row []string, headerMap map[string]int) *models.CSVLogEntry {
	entry := &models.CSVLogEntry{}

	if len(headerMap) == 0 || len(row) == 0 {
		return entry
	}

	v := reflect.ValueOf(entry).Elem() // struct value
	t := v.Type()                      // struct type

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldName := normalizeHeader(field.Name) // normalize struct field

		if idx, ok := headerMap[fieldName]; ok && idx < len(row) {
			fieldValue := row[idx]
			if fieldValue != "" {
				v.Field(i).SetString(fieldValue)
			}
		}
	}

	return entry
}

// validateHeaderMapping shows which CSV headers map to struct fields and which are unmapped
func validateHeaderMapping(csvHeaders []string, structType reflect.Type) {
	fmt.Println("\n=== Header Mapping Validation ===")

	// Build struct field map
	structFields := make(map[string]string) // normalized -> original field name
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		normalized := normalizeHeader(field.Name)
		structFields[normalized] = field.Name
	}

	// Check CSV headers
	mapped := 0
	unmapped := []string{}

	for _, csvHeader := range csvHeaders {
		normalized := normalizeHeader(csvHeader)
		if fieldName, ok := structFields[normalized]; ok {
			mapped++
			fmt.Printf("✓ CSV '%s' → Struct '%s'\n", csvHeader, fieldName)
		} else {
			unmapped = append(unmapped, csvHeader)
		}
	}

	fmt.Printf("\nSummary: %d/%d headers mapped\n", mapped, len(csvHeaders))

	if len(unmapped) > 0 {
		fmt.Printf("\n⚠ Unmapped CSV headers (%d):\n", len(unmapped))
		for _, h := range unmapped {
			fmt.Printf("  - '%s' (normalized: '%s')\n", h, normalizeHeader(h))
		}
	}

	// Check for struct fields without CSV headers
	csvHeadersMap := make(map[string]bool)
	for _, h := range csvHeaders {
		csvHeadersMap[normalizeHeader(h)] = true
	}

	missingInCSV := []string{}
	for normalized, fieldName := range structFields {
		if !csvHeadersMap[normalized] {
			missingInCSV = append(missingInCSV, fieldName)
		}
	}

	if len(missingInCSV) > 0 {
		fmt.Printf("\n⚠ Struct fields without CSV headers (%d):\n", len(missingInCSV))
		for _, f := range missingInCSV {
			fmt.Printf("  - %s (normalized: '%s')\n", f, normalizeHeader(f))
		}
	}

	fmt.Println("=== End Validation ===")
}
