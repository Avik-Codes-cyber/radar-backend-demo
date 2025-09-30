package parser

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ren3gadem4rm0t/cef-parser-go/parser"
	"superalign.ai/config"
	"superalign.ai/internal/kafka"
	"superalign.ai/models"
)

type CEFParser struct {
	metrics *ParserMetrics
}

type ParserMetrics struct {
	linesProcessed    atomic.Uint64
	linesFailed       atomic.Uint64
	messagesPublished atomic.Uint64
	messagesFailed    atomic.Uint64
}

func (m *ParserMetrics) Report() {
	fmt.Printf("Parser Metrics - Processed: %d, Failed: %d, Published: %d, Publish Failed: %d\n",
		m.linesProcessed.Load(), m.linesFailed.Load(),
		m.messagesPublished.Load(), m.messagesFailed.Load())
}

type parseJob struct {
	lineNum int
	line    string
}

func NewCEFParser() *CEFParser {
	return &CEFParser{
		metrics: &ParserMetrics{},
	}
}

func (p *CEFParser) ParseLogFileToJSON(inputPath, outputPath string) error {
	file, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	cfg := config.LoadConfig()
	producer := kafka.NewProducerFromConfig(cfg)
	defer producer.Close()

	dlq, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer dlq.Close()

	dlqWriter := bufio.NewWriter(dlq)
	defer dlqWriter.Flush()

	// Use mutex for DLQ writes from multiple goroutines
	var dlqMutex sync.Mutex

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Channels for pipeline stages
	numWorkers := runtime.NumCPU()
	if numWorkers < 2 {
		numWorkers = 2
	}

	rawLines := make(chan parseJob, numWorkers*100)
	entries := make(chan *models.CEFLogEntry, 4096)

	var wg sync.WaitGroup
	errChan := make(chan error, numWorkers+3)

	// Stage 1: File reader - sequential read, feed to parser workers
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(rawLines)

		scanner := bufio.NewScanner(file)
		// Increase buffer size for large log lines
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
	}()

	// Stage 2: Parser workers - parallel CEF parsing
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			safeParser := parser.NewParser(parser.SafeConfig())

			for job := range rawLines {
				select {
				case <-ctx.Done():
					return
				default:
				}

				cef, parseErr := safeParser.Parse(job.line)
				p.metrics.linesProcessed.Add(1)

				if parseErr != nil {
					p.metrics.linesFailed.Add(1)
					// Write unparseable line to DLQ
					dlqMutex.Lock()
					dlqWriter.WriteString(fmt.Sprintf(`{"error":"parse_failed","line":%d,"raw":"%s"}`+"\n",
						job.lineNum, escapeJSON(job.line)))
					dlqMutex.Unlock()
					continue
				}

				severity, _ := strconv.Atoi(cef.Severity)
				extMap := make(map[string]string)
				if cef.Extensions != nil {
					extJSON := cef.Extensions.AsJSON()
					json.Unmarshal([]byte(extJSON), &extMap)
				}

				entry := &models.CEFLogEntry{
					DeviceVendor:  cef.DeviceVendor,
					DeviceProduct: cef.DeviceProduct,
					SignatureID:   cef.SignatureID,
					Severity:      severity,
					Src:           extMap["src"],
					Dst:           extMap["dst"],
					Name:          cef.Name,
				}

				select {
				case entries <- entry:
				case <-ctx.Done():
					return
				}
			}
		}(i)
	}

	// Stage 3: Publisher workers - parallel Kafka publishing with batching
	numPublishers := 3
	for i := 0; i < numPublishers; i++ {
		wg.Add(1)
		go func(publisherID int) {
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
		}(i)
	}

	// Close entries channel after all parsers finish
	go func() {
		wg.Wait()
		close(entries)
	}()

	// Wait for all goroutines with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case err := <-errChan:
		cancel()
		<-done
		return err
	case <-time.After(5 * time.Minute):
		cancel()
		<-done
		return fmt.Errorf("parsing timeout after 5 minutes")
	}

	// Report metrics
	p.metrics.Report()

	return nil
}

// escapeJSON escapes a string for embedding in JSON
func escapeJSON(s string) string {
	b, _ := json.Marshal(s)
	return string(b[1 : len(b)-1]) // Remove quotes
}
