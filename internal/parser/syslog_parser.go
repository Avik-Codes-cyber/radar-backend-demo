package parser

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/leodido/go-syslog/v4"
	"github.com/leodido/go-syslog/v4/rfc3164"
	"github.com/leodido/go-syslog/v4/rfc5424"
	"superalign.ai/config"
	"superalign.ai/internal/kafka"
)

type SyslogParser struct {
	metrics *ParserMetrics
	useRFC  int // 3164 or 5424
}

// SyslogMessage represents a unified syslog message structure
type SyslogMessage struct {
	// Common fields
	Priority  *uint8     `json:"priority,omitempty"`
	Facility  *uint8     `json:"facility,omitempty"`
	Severity  *uint8     `json:"severity,omitempty"`
	Timestamp *time.Time `json:"timestamp,omitempty"`
	Hostname  *string    `json:"hostname,omitempty"`
	Appname   *string    `json:"appname,omitempty"`
	ProcID    *string    `json:"procid,omitempty"`
	MsgID     *string    `json:"msgid,omitempty"`
	Message   *string    `json:"message,omitempty"`

	// RFC5424 specific
	Version        *uint16                       `json:"version,omitempty"`
	StructuredData *map[string]map[string]string `json:"structured_data,omitempty"`

	// RFC3164 specific
	Tag *string `json:"tag,omitempty"`

	// Metadata
	RawLog string `json:"raw_log"`
	RFC    int    `json:"rfc"` // 3164 or 5424
}

func NewSyslogParser(rfc int) *SyslogParser {
	if rfc != 3164 && rfc != 5424 {
		rfc = 5424 // Default to RFC5424
	}
	return &SyslogParser{
		metrics: &ParserMetrics{},
		useRFC:  rfc,
	}
}

// ParseLogFileToKafka processes syslog file and sends to Kafka (file-based)
func (p *SyslogParser) ParseLogFileToKafka(inputPath, dlqPath string) error {
	file, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

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

	// Determine optimal worker counts
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

	// Pipeline channels
	bufferSize := cfg.ChannelBufferSize
	rawLines := make(chan parseJob, bufferSize)
	entries := make(chan map[string]string, bufferSize)

	var parserWg sync.WaitGroup
	var publisherWg sync.WaitGroup
	errChan := make(chan error, numWorkers+numPublishers+1)

	// Stage 1: File reader
	parserWg.Add(1)
	go p.fileReader(ctx, &parserWg, file, rawLines, errChan)

	// Stage 2: Parser workers
	fmt.Printf("Starting syslog pipeline (RFC%d): %d parser workers, %d publisher workers\n", p.useRFC, numWorkers, numPublishers)
	for i := 0; i < numWorkers; i++ {
		parserWg.Add(1)
		go p.parseWorker(ctx, &parserWg, rawLines, entries, errChan)
	}

	// Stage 3: Publisher workers
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

	// Wait for completion with timeout
	done := make(chan struct{})
	go func() {
		parserWg.Wait()
		publisherWg.Wait()
		close(done)
	}()

	timeoutMin := cfg.ProcessingTimeoutMin
	if timeoutMin == 0 {
		timeoutMin = 5
	}

	select {
	case <-done:
		fmt.Println("All workers completed successfully")
	case err := <-errChan:
		cancel()
		<-done
		return err
	case <-time.After(time.Duration(timeoutMin) * time.Minute):
		cancel()
		<-done
		return fmt.Errorf("parsing timeout after %d minutes", timeoutMin)
	}

	p.metrics.Report()
	return nil
}

// ParseStreamToKafka processes real-time syslog stream (for network listeners)
func (p *SyslogParser) ParseStreamToKafka(reader *bufio.Reader, dlqPath string) error {
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

	// Worker configuration
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

	bufferSize := cfg.ChannelBufferSize
	rawLines := make(chan parseJob, bufferSize)
	entries := make(chan map[string]string, bufferSize)

	var parserWg sync.WaitGroup
	var publisherWg sync.WaitGroup
	errChan := make(chan error, numWorkers+numPublishers+1)

	// Stage 1: Stream reader (continuous)
	parserWg.Add(1)
	go p.streamReader(ctx, &parserWg, reader, rawLines, errChan)

	// Stage 2: Parser workers
	fmt.Printf("Starting syslog stream pipeline (RFC%d): %d parser workers, %d publisher workers\n", p.useRFC, numWorkers, numPublishers)
	for i := 0; i < numWorkers; i++ {
		parserWg.Add(1)
		go p.parseWorker(ctx, &parserWg, rawLines, entries, errChan)
	}

	// Stage 3: Publisher workers
	for i := 0; i < numPublishers; i++ {
		publisherWg.Add(1)
		go p.publishWorker(ctx, &publisherWg, producer, cfg, entries, dlqWriter, &dlqMutex)
	}

	// Close entries channel after all parsers finish
	go func() {
		parserWg.Wait()
		close(entries)
	}()

	// Wait for completion or cancellation
	done := make(chan struct{})
	go func() {
		parserWg.Wait()
		publisherWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		fmt.Println("Stream processing completed")
	case err := <-errChan:
		cancel()
		<-done
		return err
	}

	p.metrics.Report()
	return nil
}

// fileReader reads from file line by line
func (p *SyslogParser) fileReader(ctx context.Context, wg *sync.WaitGroup, file *os.File, rawLines chan parseJob, errChan chan error) {
	defer wg.Done()
	defer close(rawLines)

	scanner := bufio.NewScanner(file)
	// Increase buffer for large log lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		select {
		case <-ctx.Done():
			return
		case rawLines <- parseJob{lineNum: lineNum, line: scanner.Text()}:
		}
	}

	if err := scanner.Err(); err != nil {
		select {
		case errChan <- fmt.Errorf("scanner error: %w", err):
		default:
		}
	}
}

// streamReader reads from continuous stream (network connection)
func (p *SyslogParser) streamReader(ctx context.Context, wg *sync.WaitGroup, reader *bufio.Reader, rawLines chan parseJob, errChan chan error) {
	defer wg.Done()
	defer close(rawLines)

	lineNum := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			// EOF or connection closed
			if err.Error() != "EOF" {
				select {
				case errChan <- fmt.Errorf("stream read error: %w", err):
				default:
				}
			}
			return
		}

		lineNum++
		select {
		case <-ctx.Done():
			return
		case rawLines <- parseJob{lineNum: lineNum, line: line}:
		}
	}
}

// parseWorker parses syslog messages in parallel
func (p *SyslogParser) parseWorker(ctx context.Context, wg *sync.WaitGroup, rawLines chan parseJob, entries chan map[string]string, errChan chan error) {
	defer wg.Done()

	// Initialize parsers with best effort mode
	var parser5424 syslog.Machine
	var parser3164 syslog.Machine

	if p.useRFC == 5424 {
		parser5424 = rfc5424.NewParser(rfc5424.WithBestEffort())
	} else {
		parser3164 = rfc3164.NewParser(rfc3164.WithBestEffort())
	}

	for job := range rawLines {
		select {
		case <-ctx.Done():
			return
		default:
		}

		p.metrics.linesProcessed.Add(1)

		// Parse the syslog message
		var entry map[string]string
		var parseErr error

		if p.useRFC == 5424 {
			entry, parseErr = p.parseRFC5424(parser5424, job.line)
		} else {
			entry, parseErr = p.parseRFC3164(parser3164, job.line)
		}

		if parseErr != nil {
			p.metrics.linesFailed.Add(1)
			continue
		}

		select {
		case entries <- entry:
		case <-ctx.Done():
			return
		}
	}
}

// parseRFC5424 parses RFC5424 format syslog message
func (p *SyslogParser) parseRFC5424(parser syslog.Machine, line string) (map[string]string, error) {
	result, err := parser.Parse([]byte(line))
	if err != nil {
		return nil, err
	}

	msg, ok := result.(*rfc5424.SyslogMessage)
	if !ok || msg == nil {
		return nil, fmt.Errorf("invalid message type or nil message")
	}

	// Convert to dynamic map
	entry := make(map[string]string)
	entry["raw_log"] = line
	entry["rfc"] = "5424"

	if msg.Priority != nil {
		entry["priority"] = fmt.Sprintf("%d", *msg.Priority)
	}
	if msg.Facility != nil {
		entry["facility"] = fmt.Sprintf("%d", *msg.Facility)
	}
	if msg.Severity != nil {
		entry["severity"] = fmt.Sprintf("%d", *msg.Severity)
	}
	if msg.Timestamp != nil {
		entry["timestamp"] = msg.Timestamp.Format(time.RFC3339)
	}
	if msg.Hostname != nil {
		entry["hostname"] = *msg.Hostname
	}
	if msg.Appname != nil {
		entry["appname"] = *msg.Appname
	}
	if msg.ProcID != nil {
		entry["procid"] = *msg.ProcID
	}
	if msg.MsgID != nil {
		entry["msgid"] = *msg.MsgID
	}
	if msg.Message != nil {
		entry["message"] = *msg.Message
	}
	if msg.Version > 0 {
		entry["version"] = fmt.Sprintf("%d", msg.Version)
	}

	// Handle structured data
	if msg.StructuredData != nil && len(*msg.StructuredData) > 0 {
		sdJSON, _ := json.Marshal(msg.StructuredData)
		entry["structured_data"] = string(sdJSON)
	}

	return entry, nil
}

// parseRFC3164 parses RFC3164 format syslog message
func (p *SyslogParser) parseRFC3164(parser syslog.Machine, line string) (map[string]string, error) {
	result, err := parser.Parse([]byte(line))
	if err != nil {
		return nil, err
	}

	msg, ok := result.(*rfc3164.SyslogMessage)
	if !ok || msg == nil {
		return nil, fmt.Errorf("invalid message type or nil message")
	}

	// Convert to dynamic map
	entry := make(map[string]string)
	entry["raw_log"] = line
	entry["rfc"] = "3164"

	if msg.Priority != nil {
		entry["priority"] = fmt.Sprintf("%d", *msg.Priority)
	}
	if msg.Facility != nil {
		entry["facility"] = fmt.Sprintf("%d", *msg.Facility)
	}
	if msg.Severity != nil {
		entry["severity"] = fmt.Sprintf("%d", *msg.Severity)
	}
	if msg.Timestamp != nil {
		entry["timestamp"] = msg.Timestamp.Format(time.RFC3339)
	}
	if msg.Hostname != nil {
		entry["hostname"] = *msg.Hostname
	}
	if msg.Appname != nil {
		entry["appname"] = *msg.Appname
	}
	if msg.ProcID != nil {
		entry["procid"] = *msg.ProcID
	}
	if msg.MsgID != nil {
		entry["msgid"] = *msg.MsgID
	}
	if msg.Message != nil {
		entry["message"] = *msg.Message

		// Extract FortiGate key=value pairs from message
		fortiFields := parseFortiRFC3164Message(*msg.Message)
		for key, value := range fortiFields {
			// Prefix with "forti_" to distinguish from standard syslog fields
			entry["forti_"+key] = value
		}
	}

	return entry, nil
}

// parseFortiRFC3164Message extracts key=value pairs from FortiGate log messages
func parseFortiRFC3164Message(msg string) map[string]string {
	result := make(map[string]string)
	for _, kv := range strings.Fields(msg) {
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) == 2 {
			result[parts[0]] = parts[1]
		}
	}
	return result
}

// publishWorker publishes messages to Kafka with batching and retries
func (p *SyslogParser) publishWorker(ctx context.Context, wg *sync.WaitGroup, producer *kafka.Producer,
	cfg *config.Config, entries chan map[string]string, dlqWriter *bufio.Writer, dlqMutex *sync.Mutex) {
	defer wg.Done()

	const (
		maxRetries   = 3
		retryBackoff = 250 * time.Millisecond
	)

	flushInterval := time.Duration(cfg.PublishFlushIntervalMs) * time.Millisecond

	maxBatchSize := cfg.KafkaBatchSize
	if maxBatchSize == 0 {
		maxBatchSize = 500
	}

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
				fmt.Printf("Published batch of %d syslog messages to Kafka topic '%s' (total: %d)\n", batchSize, cfg.KafkaTopic, totalPublished)
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
