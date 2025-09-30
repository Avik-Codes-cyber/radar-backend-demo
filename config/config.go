package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config holds all configuration for the log processing pipeline
type Config struct {
	// Kafka Configuration
	KafkaBrokers       []string `json:"kafka_brokers"`
	KafkaTopic         string   `json:"kafka_topic"`
	KafkaConsumerGroup string   `json:"kafka_consumer_group"`

	// ClickHouse Configuration
	ClickHouseHost     string `json:"clickhouse_host"`
	ClickHousePort     int    `json:"clickhouse_port"`
	ClickHouseDatabase string `json:"clickhouse_database"`
	ClickHouseUsername string `json:"clickhouse_username"`
	ClickHousePassword string `json:"clickhouse_password"`
	ClickHouseTable    string `json:"clickhouse_table"`

	// Supabase Configuration
	SupabaseURL        string `json:"supabase_url"`
	SupabaseAnonKey    string `json:"supabase_anon_key"`
	SupabaseServiceKey string `json:"supabase_service_key"`

	// Logging Configuration
	LogLevel string `json:"log_level"`

	// Performance Configuration
	ParserWorkers        int `json:"parser_workers"`         // Number of parallel parser workers (0 = auto)
	PublisherWorkers     int `json:"publisher_workers"`      // Number of Kafka publisher workers (0 = auto)
	ParquetBatchSize     int `json:"parquet_batch_size"`     // Rows to read per Parquet batch
	KafkaBatchSize       int `json:"kafka_batch_size"`       // Messages per Kafka batch
	ProcessingTimeoutMin int `json:"processing_timeout_min"` // Max processing time in minutes
	ChannelBufferSize    int `json:"channel_buffer_size"`    // Size of internal channels
}

// LoadConfig loads configuration from environment variables
func LoadConfig() *Config {
	return &Config{
		// Kafka Configuration
		KafkaBrokers:       strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaTopic:         getEnv("KAFKA_TOPIC", "log-processing"),
		KafkaConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "log-processor"),

		// ClickHouse Configuration
		ClickHouseHost:     getEnv("CLICKHOUSE_HOST", "localhost"),
		ClickHousePort:     getEnvAsInt("CLICKHOUSE_PORT", 9000),
		ClickHouseDatabase: getEnv("CLICKHOUSE_DATABASE", "logs"),
		ClickHouseUsername: getEnv("CLICKHOUSE_USERNAME", "default"),
		ClickHousePassword: getEnv("CLICKHOUSE_PASSWORD", ""),
		ClickHouseTable:    getEnv("CLICKHOUSE_TABLE", "processed_logs"),

		// Supabase Configuration
		SupabaseURL:        getEnv("SUPABASE_URL", ""),
		SupabaseAnonKey:    getEnv("SUPABASE_ANON_KEY", ""),
		SupabaseServiceKey: getEnv("SUPABASE_SERVICE_KEY", ""),

		// Logging Configuration
		LogLevel: getEnv("LOG_LEVEL", "info"),

		// Performance Configuration
		ParserWorkers:        getEnvAsInt("PARSER_WORKERS", 0),           // 0 = auto (NumCPU)
		PublisherWorkers:     getEnvAsInt("PUBLISHER_WORKERS", 0),        // 0 = auto (NumCPU/2, min 3)
		ParquetBatchSize:     getEnvAsInt("PARQUET_BATCH_SIZE", 1000),    // Read 1000 rows at a time
		KafkaBatchSize:       getEnvAsInt("KAFKA_BATCH_SIZE", 500),       // Send 500 messages per batch
		ProcessingTimeoutMin: getEnvAsInt("PROCESSING_TIMEOUT_MIN", 120), // 2 hours default
		ChannelBufferSize:    getEnvAsInt("CHANNEL_BUFFER_SIZE", 10000),  // 10k buffer
	}
}

// Helper functions for environment variable parsing
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	valueStr := getEnv(key, "")
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}
	return defaultValue
}

func getEnvAsBool(key string, defaultValue bool) bool {
	valueStr := getEnv(key, "")
	if value, err := strconv.ParseBool(valueStr); err == nil {
		return value
	}
	return defaultValue
}

func getEnvAsFloat(key string, defaultValue float64) float64 {
	valueStr := getEnv(key, "")
	if value, err := strconv.ParseFloat(valueStr, 64); err == nil {
		return value
	}
	return defaultValue
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if len(c.KafkaBrokers) == 0 {
		return fmt.Errorf("kafka brokers must be specified")
	}
	if c.KafkaTopic == "" {
		return fmt.Errorf("kafka topic must be specified")
	}
	if c.ClickHouseHost == "" {
		return fmt.Errorf("clickhouse host must be specified")
	}
	if c.SupabaseURL == "" {
		return fmt.Errorf("supabase URL must be specified when AI filtering is enabled")
	}
	return nil
}
