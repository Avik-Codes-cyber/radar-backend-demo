package server

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"superalign.ai/config"
	"superalign.ai/internal/parser"
)

// SyslogServer listens for incoming syslog messages over UDP or TCP
type SyslogServer struct {
	cfg        *config.Config
	protocol   string // "udp" or "tcp"
	port       int
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
}

// NewSyslogServer creates a new syslog server instance
func NewSyslogServer(cfg *config.Config, protocol string, port int) *SyslogServer {
	if protocol != "udp" && protocol != "tcp" {
		protocol = "udp" // Default to UDP (most common for Fortigate)
	}
	if port == 0 {
		port = 514 // Default syslog port
	}

	return &SyslogServer{
		cfg:      cfg,
		protocol: protocol,
		port:     port,
	}
}

// Start begins listening for syslog messages
func (s *SyslogServer) Start(ctx context.Context) error {
	ctx, s.cancelFunc = context.WithCancel(ctx)

	addr := fmt.Sprintf(":%d", s.port)
	fmt.Printf("Starting syslog server on %s %s...\n", s.protocol, addr)

	if s.protocol == "udp" {
		return s.startUDP(ctx, addr)
	}
	return s.startTCP(ctx, addr)
}

// Stop gracefully stops the syslog server
func (s *SyslogServer) Stop() {
	if s.cancelFunc != nil {
		s.cancelFunc()
	}
	s.wg.Wait()
	fmt.Println("Syslog server stopped")
}

// startUDP handles UDP syslog connections
func (s *SyslogServer) startUDP(ctx context.Context, addr string) error {
	conn, err := net.ListenPacket("udp", addr)
	if err != nil {
		return fmt.Errorf("failed to start UDP listener: %w", err)
	}
	defer conn.Close()

	fmt.Printf("Syslog server listening on UDP %s\n", addr)
	fmt.Println("Ready to receive logs from Fortigate...")

	// Create DLQ file
	dlqPath := filepath.Join("uploads", fmt.Sprintf("syslog_dlq_%s.log", time.Now().Format("20060102_150405")))

	// Buffer for UDP packets (max syslog message size is typically 1024 bytes for UDP)
	buffer := make([]byte, 65535)

	// Use a channel to aggregate messages for batch processing
	messageChan := make(chan string, s.cfg.ChannelBufferSize)

	// Start message processor goroutine
	s.wg.Add(1)
	go s.processMessages(ctx, messageChan, dlqPath)

	// Main UDP receive loop
	for {
		select {
		case <-ctx.Done():
			close(messageChan)
			return nil
		default:
		}

		// Set read deadline to allow checking context
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))

		n, addr, err := conn.ReadFrom(buffer)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue // Timeout is expected, check context and continue
			}
			log.Printf("UDP read error: %v\n", err)
			continue
		}

		message := string(buffer[:n])
		message = strings.TrimSpace(message)

		if message != "" {
			select {
			case messageChan <- message:
				// Successfully queued
			case <-ctx.Done():
				close(messageChan)
				return nil
			default:
				log.Printf("Message channel full, dropping message from %s\n", addr)
			}
		}
	}
}

// startTCP handles TCP syslog connections
func (s *SyslogServer) startTCP(ctx context.Context, addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start TCP listener: %w", err)
	}
	defer listener.Close()

	fmt.Printf("Syslog server listening on TCP %s\n", addr)
	fmt.Println("Ready to receive logs from Fortigate...")

	// Accept connections in a loop
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Set accept deadline to allow checking context
			listener.(*net.TCPListener).SetDeadline(time.Now().Add(1 * time.Second))

			conn, err := listener.Accept()
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				log.Printf("TCP accept error: %v\n", err)
				continue
			}

			// Handle each connection in a separate goroutine
			s.wg.Add(1)
			go s.handleTCPConnection(ctx, conn)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()
	return nil
}

// handleTCPConnection processes a single TCP connection
func (s *SyslogServer) handleTCPConnection(ctx context.Context, conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()
	fmt.Printf("New TCP connection from %s\n", remoteAddr)

	// Create DLQ file for this connection
	dlqPath := filepath.Join("uploads", fmt.Sprintf("syslog_dlq_%s_%s.log",
		time.Now().Format("20060102_150405"),
		strings.Replace(remoteAddr, ":", "_", -1)))

	// Use the existing parser's ParseStreamToKafka method
	reader := bufio.NewReader(conn)

	// Detect format from first message
	format := s.detectFormat(reader)

	var err error
	if format == "CEF" {
		fmt.Printf("Detected CEF format from %s, using CEF parser...\n", remoteAddr)
		cefParser := parser.NewCEFParser()
		// For CEF, we need to implement a stream parser (similar to syslog)
		err = s.processCEFStream(ctx, reader, dlqPath, cefParser)
	} else {
		// Default to syslog parser (RFC5424)
		fmt.Printf("Using Syslog parser (RFC5424) for connection from %s...\n", remoteAddr)
		syslogParser := parser.NewSyslogParser(5424)
		err = syslogParser.ParseStreamToKafka(reader, dlqPath)
	}

	if err != nil {
		log.Printf("Error processing stream from %s: %v\n", remoteAddr, err)
	}
	fmt.Printf("Connection closed from %s\n", remoteAddr)
}

// processMessages handles the message queue and routes to appropriate parser
func (s *SyslogServer) processMessages(ctx context.Context, messageChan chan string, dlqPath string) {
	defer s.wg.Done()

	// Create a temporary file to buffer messages, then process in batches
	tempFile, err := os.CreateTemp("uploads", "syslog_buffer_*.log")
	if err != nil {
		log.Printf("Failed to create temp buffer file: %v\n", err)
		return
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	writer := bufio.NewWriter(tempFile)
	ticker := time.NewTicker(1 * time.Second) // Process batch every second
	defer ticker.Stop()

	messageCount := 0
	var format string

	flush := func() {
		if messageCount == 0 {
			return
		}

		writer.Flush()
		tempFile.Seek(0, 0) // Reset to beginning

		// Process the buffered messages
		if format == "CEF" {
			fmt.Printf("Processing %d CEF messages...\n", messageCount)
			cefParser := parser.NewCEFParser()
			if err := cefParser.ParseLogFileToJSON(tempFile.Name(), dlqPath); err != nil {
				log.Printf("CEF parsing error: %v\n", err)
			}
		} else {
			fmt.Printf("Processing %d syslog messages...\n", messageCount)
			syslogParser := parser.NewSyslogParser(5424)
			if err := syslogParser.ParseLogFileToKafka(tempFile.Name(), dlqPath); err != nil {
				log.Printf("Syslog parsing error: %v\n", err)
			}
		}

		// Reset buffer
		tempFile.Truncate(0)
		tempFile.Seek(0, 0)
		writer.Reset(tempFile)
		messageCount = 0
	}

	for {
		select {
		case message, ok := <-messageChan:
			if !ok {
				flush()
				return
			}

			// Detect format from first message
			if messageCount == 0 {
				format = detectMessageFormat(message)
			}

			// Write to buffer
			writer.WriteString(message)
			writer.WriteString("\n")
			messageCount++

			// Flush if batch size reached
			if messageCount >= s.cfg.KafkaBatchSize {
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

// detectFormat attempts to detect the log format from the stream
func (s *SyslogServer) detectFormat(reader *bufio.Reader) string {
	// Peek at the first message to detect format
	peek, err := reader.Peek(1024)
	if err != nil && err != io.EOF {
		return "SYSLOG" // Default to syslog
	}

	message := string(peek)
	return detectMessageFormat(message)
}

// detectMessageFormat detects format from a single message
func detectMessageFormat(message string) string {
	// Check for CEF format
	if strings.Contains(message, "CEF:") {
		return "CEF"
	}

	// Check for RFC5424 format (has version number after priority)
	if len(message) > 0 && message[0] == '<' {
		if idx := strings.Index(message, ">"); idx > 0 && idx < 10 {
			rest := message[idx+1:]
			if len(rest) > 0 && rest[0] >= '0' && rest[0] <= '9' {
				return "RFC5424"
			}
			return "RFC3164"
		}
	}

	// Default to syslog
	return "SYSLOG"
}

// processCEFStream processes a stream of CEF messages (similar to syslog stream processing)
func (s *SyslogServer) processCEFStream(ctx context.Context, reader *bufio.Reader, dlqPath string, cefParser *parser.CEFParser) error {
	// Create a temporary file to buffer messages
	tempFile, err := os.CreateTemp("uploads", "cef_stream_*.log")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	writer := bufio.NewWriter(tempFile)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	lineCount := 0
	maxBatchSize := s.cfg.KafkaBatchSize

	flush := func() error {
		if lineCount == 0 {
			return nil
		}

		writer.Flush()
		tempFile.Seek(0, 0)

		// Process the buffered CEF messages
		if err := cefParser.ParseLogFileToJSON(tempFile.Name(), dlqPath); err != nil {
			return err
		}

		// Reset buffer
		tempFile.Truncate(0)
		tempFile.Seek(0, 0)
		writer.Reset(tempFile)
		lineCount = 0
		return nil
	}

	// Read lines from stream
	scanner := bufio.NewScanner(reader)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			flush()
			return nil
		default:
		}

		line := scanner.Text()
		writer.WriteString(line)
		writer.WriteString("\n")
		lineCount++

		if lineCount >= maxBatchSize {
			if err := flush(); err != nil {
				log.Printf("Error flushing batch: %v\n", err)
			}
		}
	}

	// Final flush
	flush()

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}

	return nil
}
