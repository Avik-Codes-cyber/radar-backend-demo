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
	"sync/atomic"
	"time"

	"github.com/ren3gadem4rm0t/cef-parser-go/parser"
	"superalign.ai/config"
	"superalign.ai/internal/kafka"
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
	rawLines := make(chan parseJob, bufferSize)
	entries := make(chan map[string]string, bufferSize)

	var parserWg sync.WaitGroup
	var publisherWg sync.WaitGroup
	errChan := make(chan error, numWorkers+numPublishers+1)

	// Stage 1: File reader - sequential read, feed to parser workers
	parserWg.Add(1)
	go func() {
		defer parserWg.Done()
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
	fmt.Printf("Starting CEF pipeline: %d parser workers, %d publisher workers\n", numWorkers, numPublishers)
	for i := 0; i < numWorkers; i++ {
		parserWg.Add(1)
		go func(workerID int) {
			defer parserWg.Done()

			safeParser := parser.NewParser(parser.SafeConfig())

			for job := range rawLines {
				select {
				case <-ctx.Done():
					return
				default:
				}

				// Preprocess FortiGate CEF format (strip syslog wrapper, normalize)
				line := PreprocessFortigateCEF(job.line)

				cef, parseErr := safeParser.Parse(line)
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

				// Build dynamic entry map
				entry := make(map[string]string)
				entry["raw_log"] = job.line
				entry["format"] = "CEF"
				entry["device_vendor"] = cef.DeviceVendor
				entry["device_product"] = cef.DeviceProduct
				entry["signature_id"] = cef.SignatureID
				entry["severity"] = cef.Severity
				entry["name"] = cef.Name

				// Add all extensions as fields
				if cef.Extensions != nil {
					extJSON := cef.Extensions.AsJSON()
					var extMap map[string]interface{}
					if json.Unmarshal([]byte(extJSON), &extMap) == nil {
						for k, v := range extMap {
							value := fmt.Sprintf("%v", v)
							entry[k] = value

							// Strip FTNTFGT prefix from FortiGate fields (e.g., FTNTFGTlogid → logid)
							cleanKey := k
							if strings.HasPrefix(k, "FTNTFGT") {
								cleanKey = strings.TrimPrefix(k, "FTNTFGT")
								entry[cleanKey] = value
							}

							// Map CEF standard fields to FortiGate field names for compatibility
							switch k {
							case "src":
								entry["srcip"] = value
							case "dst":
								entry["dstip"] = value
							case "spt":
								entry["srcport"] = value
							case "dpt":
								entry["dstport"] = value
							case "act":
								entry["action"] = value
							case "deviceHostName":
								entry["devid"] = value
							case "cs1": // Custom string 1 often used for URL
								entry["url"] = value
							case "destinationDnsDomain", "dhost":
								entry["dstname"] = value
							case "sourceHostName", "shost":
								entry["srcname"] = value
							case "deviceProcessName", "sproc":
								entry["app"] = value
							case "rt": // Receipt time (timestamp)
								entry["eventtime"] = value
							case "request": // request URL
								if entry["url"] == "" {
									entry["url"] = value
								}
							}
						}
					}
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
	for i := 0; i < numPublishers; i++ {
		publisherWg.Add(1)
		go p.cefPublishWorker(ctx, &publisherWg, producer, cfg, entries, dlqWriter, &dlqMutex)
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

// cefPublishWorker publishes CEF messages to Kafka with batching and retries
func (p *CEFParser) cefPublishWorker(ctx context.Context, wg *sync.WaitGroup, producer *kafka.Producer,
	cfg *config.Config, entries chan map[string]string, dlqWriter *bufio.Writer, dlqMutex *sync.Mutex) {
	defer wg.Done()

	const (
		flushInterval = 500 * time.Millisecond
		maxRetries    = 3
		retryBackoff  = 250 * time.Millisecond
	)

	// Use configurable batch size
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
				fmt.Printf("Published batch of %d CEF messages to Kafka topic '%s' (total: %d)\n", batchSize, cfg.KafkaTopic, totalPublished)
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

func PreprocessFortigateCEF(line string) string {
	// 1️⃣ Find and extract the CEF portion (with CEF: prefix)
	cefIdx := strings.Index(line, "CEF:")
	if cefIdx == -1 {
		// Try with space
		cefIdx = strings.Index(line, "CEF: ")
		if cefIdx == -1 {
			// No CEF marker found, return as-is
			return line
		}
	}

	// Extract from CEF: onwards
	line = strings.TrimSpace(line[cefIdx:])

	// 2️⃣ Normalize "CEF: " to "CEF:" (space after colon)
	line = strings.Replace(line, "CEF: ", "CEF:", 1)

	// 3️⃣ Check if this is valid CEF format
	if !strings.HasPrefix(line, "CEF:") {
		return line
	}

	withoutPrefix := strings.TrimPrefix(line, "CEF:")

	// 4️⃣ Split into exactly 8 parts (Version + 7 pipe-separated fields)
	// CEF format: Version|Device Vendor|Device Product|Device Version|Signature ID|Name|Severity|Extension
	parts := strings.SplitN(withoutPrefix, "|", 8)

	// Need at least 8 parts for valid CEF
	if len(parts) < 8 {
		// Pad with empty strings if needed
		for len(parts) < 8 {
			parts = append(parts, "")
		}
	}

	// 5️⃣ Normalize the Name field (index 5, the 6th field after version)
	// FortiGate often has spaces and special chars in Name field which violates CEF spec
	if len(parts) > 0 && len(parts) > 5 {
		parts[5] = strings.TrimSpace(parts[5])
		// Replace problematic characters
		parts[5] = strings.ReplaceAll(parts[5], " ", "_")
		parts[5] = strings.ReplaceAll(parts[5], ":", "_")
		parts[5] = strings.ReplaceAll(parts[5], "|", "_")
		parts[5] = strings.ReplaceAll(parts[5], "=", "_")
	}

	// 6️⃣ Clean up Severity field (index 6)
	if len(parts) > 6 {
		parts[6] = strings.TrimSpace(parts[6])
	}

	// 7️⃣ Clean up extension field (index 7)
	if len(parts) > 7 {
		extension := strings.TrimSpace(parts[7])

		// Handle extension fields with spaces: CEF requires spaces in values to be escaped
		// Parse key=value pairs and escape spaces in all values
		var result []string
		tokens := strings.Split(extension, " ")

		i := 0
		for i < len(tokens) {
			token := tokens[i]

			// Check if this token contains '=' (it's a key=value pair)
			if strings.Contains(token, "=") {
				kvParts := strings.SplitN(token, "=", 2)
				if len(kvParts) == 2 {
					key := kvParts[0]
					value := kvParts[1]

					// Collect all tokens until we hit the next key=value pair
					j := i + 1
					for j < len(tokens) && !strings.Contains(tokens[j], "=") {
						value += " " + tokens[j]
						j++
					}

					// Replace spaces with underscores in the value (CEF spec compliance)
					valueNormalized := strings.ReplaceAll(value, " ", "_")
					result = append(result, key+"="+valueNormalized)
					i = j
					continue
				}
			}

			i++
		}

		if len(result) > 0 {
			extension = strings.Join(result, " ")
		}

		parts[7] = extension
	}

	// 8️⃣ Reconstruct with CEF: prefix
	result := "CEF:" + strings.Join(parts, "|")

	return result
}

// escapeJSON escapes a string for embedding in JSON
func escapeJSON(s string) string {
	b, _ := json.Marshal(s)
	return string(b[1 : len(b)-1]) // Remove quotes
}
