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
	"runtime"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"superalign.ai/config"
	"superalign.ai/internal/kafka"
	"superalign.ai/models"
)

type CSVParser struct {
	metrics *ParserMetrics
}

type csvParseJob struct {
	rowNum int
	row    []string
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

	numWorkers := runtime.NumCPU()
	if numWorkers < 2 {
		numWorkers = 2
	}

	rawRows := make(chan csvParseJob, numWorkers*100)
	entries := make(chan *models.CSVLogEntry, 4096)

	var wg sync.WaitGroup
	errChan := make(chan error, numWorkers+3)

	// Stage 1: Reader - either CSV or Parquet
	wg.Add(1)
	if useParquet {
		go p.parquetReader(ctx, &wg, inputPath, rawRows, errChan)
	} else {
		go p.csvReader(ctx, &wg, inputPath, rawRows, errChan)
	}

	// Stage 2: Parser workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

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

				entry := &models.CSVLogEntry{}
				// Map row to struct fields (assuming order matches headers)
				if len(job.row) >= 111 {
					entry.Timestamp = job.row[0]
					entry.Source = job.row[1]
					entry.Message = job.row[2]
					entry.Header1DeviceVendor = job.row[3]
					entry.Header2DeviceProduct = job.row[4]
					entry.Header4DeviceEventClassId = job.row[5]
					entry.Header5Name = job.row[6]
					entry.Duser = job.row[7]
					entry.Cn1 = job.row[8]
					entry.Dpt = job.row[9]
					entry.Dst = job.row[10]
					entry.Out = job.row[11]
					entry.Act = job.row[12]
					entry.EventId = job.row[13]
					entry.Type = job.row[14]
					entry.Start = job.row[15]
					entry.End = job.row[16]
					entry.App = job.row[17]
					entry.Proto = job.row[18]
					entry.Art = job.row[19]
					entry.Cat = job.row[20]
					entry.DeviceSeverity = job.row[21]
					entry.Rt = job.row[22]
					entry.Src = job.row[23]
					entry.SourceZoneURI = job.row[24]
					entry.Spt = job.row[25]
					entry.Dhost = job.row[26]
					entry.DestinationZoneURI = job.row[27]
					entry.Request = job.row[28]
					entry.RequestContext = job.row[29]
					entry.RequestMethod = job.row[30]
					entry.Ahost = job.row[31]
					entry.Agt = job.row[32]
					entry.DeviceExternalId = job.row[33]
					entry.DeviceInboundInterface = job.row[34]
					entry.DeviceOutboundInterface = job.row[35]
					entry.Suser = job.row[36]
					entry.Shost = job.row[37]
					entry.Reason = job.row[38]
					entry.RequestClientApplication = job.row[39]
					entry.Cnt = job.row[40]
					entry.Amac = job.row[41]
					entry.Atz = job.row[42]
					entry.Dvchost = job.row[43]
					entry.Dtz = job.row[44]
					entry.Dvc = job.row[45]
					entry.DeviceZoneURI = job.row[46]
					entry.DeviceFacility = job.row[47]
					entry.ExternalId = job.row[48]
					entry.Msg = job.row[49]
					entry.At = job.row[50]
					entry.CategorySignificance = job.row[51]
					entry.CategoryBehavior = job.row[52]
					entry.CategoryDeviceGroup = job.row[53]
					entry.Catdt = job.row[54]
					entry.CategoryOutcome = job.row[55]
					entry.CategoryObject = job.row[56]
					entry.Cs1 = job.row[57]
					entry.Cs2 = job.row[58]
					entry.Cs3 = job.row[59]
					entry.Cs4 = job.row[60]
					entry.Cs5 = job.row[61]
					entry.Cs6 = job.row[62]
					entry.Cs1Label = job.row[63]
					entry.Cs2Label = job.row[64]
					entry.Cs3Label = job.row[65]
					entry.Cs4Label = job.row[66]
					entry.Cs5Label = job.row[67]
					entry.Cs6Label = job.row[68]
					entry.Cn1Label = job.row[69]
					entry.Cn2 = job.row[70]
					entry.Cn2Label = job.row[71]
					entry.Cn3 = job.row[72]
					entry.Cn3Label = job.row[73]
					entry.Cn4 = job.row[74]
					entry.Cn4Label = job.row[75]
					entry.Cn5 = job.row[76]
					entry.Cn5Label = job.row[77]
					entry.Cn6 = job.row[78]
					entry.Cn6Label = job.row[79]
					entry.C6a2Label = job.row[80]
					entry.C6a3Label = job.row[81]
					entry.C6a2 = job.row[82]
					entry.C6a3 = job.row[83]
					entry.DeviceProcessName = job.row[84]
					entry.Duid = job.row[85]
					entry.Suid = job.row[86]
					entry.Spid = job.row[87]
					entry.Dproc = job.row[88]
					entry.Sproc = job.row[89]
					entry.Outcome = job.row[90]
					entry.DestinationServiceName = job.row[91]
					entry.Dpriv = job.row[92]
					entry.OldFileId = job.row[93]
					entry.OldFileHash = job.row[94]
					entry.Fname = job.row[95]
					entry.FileId = job.row[96]
					entry.FileType = job.row[97]
					entry.SourceTranslatedAddress = job.row[98]
					entry.SourceTranslatedPort = job.row[99]
					entry.In = job.row[100]
					entry.Smac = job.row[101]
					entry.Dmac = job.row[102]
					entry.DeviceDirection = job.row[103]
					entry.Dntdom = job.row[104]
					entry.DeviceTranslatedAddress = job.row[105]
					entry.DstName = job.row[106]
					entry.DestinationName = job.row[107]
					entry.URL = job.row[108]
				}

				select {
				case entries <- entry:
				case <-ctx.Done():
					return
				}
			}
		}(i)
	}

	// Stage 3: Publisher workers
	numPublishers := 3
	for i := 0; i < numPublishers; i++ {
		wg.Add(1)
		go p.publishWorker(ctx, &wg, producer, cfg, entries, dlqWriter, &dlqMutex)
	}

	// Close entries after all parsers finish
	go func() {
		wg.Wait()
		close(entries)
	}()

	// Wait with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case err := <-errChan:
		cancel()
		<-done
		return err
	case <-time.After(30 * time.Minute):
		cancel()
		<-done
		return fmt.Errorf("parsing timeout after 30 minutes")
	}

	p.metrics.Report()
	return nil
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

	// Skip header
	if _, err := csvReader.Read(); err != nil {
		select {
		case errChan <- fmt.Errorf("failed to read header: %w", err):
		default:
		}
		return
	}

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
		case rawRows <- csvParseJob{rowNum: rowNum, row: append([]string(nil), row...)}:
		}
	}
}

func (p *CSVParser) parquetReader(ctx context.Context, wg *sync.WaitGroup, inputPath string, rawRows chan csvParseJob, errChan chan error) {
	defer wg.Done()
	defer close(rawRows)

	fr, err := local.NewLocalFileReader(inputPath)
	if err != nil {
		select {
		case errChan <- fmt.Errorf("failed to open parquet: %w", err):
		default:
		}
		return
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, new(models.CSVLogEntry), 4)
	if err != nil {
		select {
		case errChan <- fmt.Errorf("failed to create parquet reader: %w", err):
		default:
		}
		return
	}
	defer pr.ReadStop()

	rowNum := 0
	num := int(pr.GetNumRows())

	for i := 0; i < num; i++ {
		entries := make([]models.CSVLogEntry, 1)
		if err := pr.Read(&entries); err != nil {
			continue
		}

		rowNum++
		entry := entries[0]
		row := p.entryToRow(&entry)

		select {
		case <-ctx.Done():
			return
		case rawRows <- csvParseJob{rowNum: rowNum, row: row}:
		}
	}
}

func (p *CSVParser) publishWorker(ctx context.Context, wg *sync.WaitGroup, producer *kafka.Producer,
	cfg *config.Config, entries chan *models.CSVLogEntry, dlqWriter *bufio.Writer, dlqMutex *sync.Mutex) {
	defer wg.Done()

	const (
		maxBatchSize  = 500
		flushInterval = 500 * time.Millisecond
		maxRetries    = 3
		retryBackoff  = 250 * time.Millisecond
	)

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
				batch = batch[:0]
				return
			}

			if attempt < maxRetries-1 {
				time.Sleep(retryBackoff * time.Duration(attempt+1))
			}
		}

		// All retries failed - write to DLQ
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

func (p *CSVParser) entryToRow(entry *models.CSVLogEntry) []string {
	return []string{
		entry.Timestamp, entry.Source, entry.Message, entry.Header1DeviceVendor,
		entry.Header2DeviceProduct, entry.Header4DeviceEventClassId, entry.Header5Name,
		entry.Duser, entry.Cn1, entry.Dpt, entry.Dst, entry.Out, entry.Act, entry.EventId,
		entry.Type, entry.Start, entry.End, entry.App, entry.Proto, entry.Art, entry.Cat,
		entry.DeviceSeverity, entry.Rt, entry.Src, entry.SourceZoneURI, entry.Spt,
		entry.Dhost, entry.DestinationZoneURI, entry.Request, entry.RequestContext,
		entry.RequestMethod, entry.Ahost, entry.Agt, entry.DeviceExternalId,
		entry.DeviceInboundInterface, entry.DeviceOutboundInterface, entry.Suser,
		entry.Shost, entry.Reason, entry.RequestClientApplication, entry.Cnt, entry.Amac,
		entry.Atz, entry.Dvchost, entry.Dtz, entry.Dvc, entry.DeviceZoneURI,
		entry.DeviceFacility, entry.ExternalId, entry.Msg, entry.At,
		entry.CategorySignificance, entry.CategoryBehavior, entry.CategoryDeviceGroup,
		entry.Catdt, entry.CategoryOutcome, entry.CategoryObject, entry.Cs1, entry.Cs2,
		entry.Cs3, entry.Cs4, entry.Cs5, entry.Cs6, entry.Cs1Label, entry.Cs2Label,
		entry.Cs3Label, entry.Cs4Label, entry.Cs5Label, entry.Cs6Label, entry.Cn1Label,
		entry.Cn2, entry.Cn2Label, entry.Cn3, entry.Cn3Label, entry.Cn4, entry.Cn4Label,
		entry.Cn5, entry.Cn5Label, entry.Cn6, entry.Cn6Label, entry.C6a2Label,
		entry.C6a3Label, entry.C6a2, entry.C6a3, entry.DeviceProcessName, entry.Duid,
		entry.Suid, entry.Spid, entry.Dproc, entry.Sproc, entry.Outcome,
		entry.DestinationServiceName, entry.Dpriv, entry.OldFileId, entry.OldFileHash,
		entry.Fname, entry.FileId, entry.FileType, entry.SourceTranslatedAddress,
		entry.SourceTranslatedPort, entry.In, entry.Smac, entry.Dmac, entry.DeviceDirection,
		entry.Dntdom, entry.DeviceTranslatedAddress, entry.DstName, entry.DestinationName, entry.URL,
	}
}
