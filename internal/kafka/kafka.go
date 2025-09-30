package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"superalign.ai/config"
)

// Producer wraps a kafka-go writer for producing messages
type Producer struct {
	writer       *kafka.Writer
	defaultTopic string
}

// NewProducerFromConfig creates a Producer using brokers and topic from config
func NewProducerFromConfig(cfg *config.Config) *Producer {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.KafkaBrokers...),
		Topic:                  cfg.KafkaTopic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
	return &Producer{writer: w, defaultTopic: cfg.KafkaTopic}
}

// PublishJSON writes a single JSON payload to the given topic (or default if empty)
func (p *Producer) PublishJSON(ctx context.Context, topic string, payload []byte) error {
	// When Writer.Topic is set, Message.Topic must be empty per kafka-go contract
	msg := kafka.Message{Value: payload}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return p.writer.WriteMessages(ctx, msg)
}

// Close flushes and closes the underlying writer
func (p *Producer) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}
