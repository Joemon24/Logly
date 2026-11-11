package dispatcher

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const kafkaWriteTimeout = 10 * time.Second

// KafkaConfig describes one explicit broker set and topic. There are no
// implicit defaults: a missing or empty value is rejected rather than silently
// connecting to localhost or a shared broker.
type KafkaConfig struct {
	Brokers []string
	Topic   string
}

// KafkaProducer is the production Producer. Its writer requires acknowledgements
// from the full in-sync replica set and uses the message key for partition
// assignment, so all events with the same event_id key map to one partition.
type KafkaProducer struct {
	writer *kafka.Writer
}

// NewKafkaProducer validates cfg and creates a producer. Brokers must be
// host:port pairs; wildcard listen addresses are rejected because they cannot
// be used as a destination. Topic creation is disabled so a typo cannot create
// an unintended topic.
func NewKafkaProducer(cfg KafkaConfig) (*KafkaProducer, error) {
	brokers, err := normalizeBrokers(cfg.Brokers)
	if err != nil {
		return nil, err
	}
	topic, err := normalizeTopic(cfg.Topic)
	if err != nil {
		return nil, err
	}

	return &KafkaProducer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			RequiredAcks:           kafka.RequireAll,
			Balancer:               &kafka.Hash{},
			AllowAutoTopicCreation: false,
			Async:                  false,
			MaxAttempts:            1,
			ReadTimeout:            kafkaWriteTimeout,
			WriteTimeout:           kafkaWriteTimeout,
		},
	}, nil
}

// Publish sends the batch and returns only after the broker acknowledges it.
// Message values are passed through as-is.
func (p *KafkaProducer) Publish(ctx context.Context, messages []Message) error {
	if len(messages) == 0 {
		return nil
	}
	batch := make([]kafka.Message, len(messages))
	for i, message := range messages {
		batch[i] = kafka.Message{
			Key:   message.Key,
			Value: message.Value,
		}
	}
	if err := p.writer.WriteMessages(ctx, batch...); err != nil {
		return fmt.Errorf("kafka write: %w", err)
	}
	return nil
}

// Close flushes the writer and releases broker connections.
func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}

func normalizeBrokers(brokers []string) ([]string, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("%w: at least one kafka broker is required", ErrInvalidConfig)
	}
	normalized := make([]string, 0, len(brokers))
	for _, broker := range brokers {
		broker = strings.TrimSpace(broker)
		if broker == "" {
			return nil, fmt.Errorf("%w: kafka broker must not be empty", ErrInvalidConfig)
		}
		if err := validateBrokerAddress(broker); err != nil {
			return nil, err
		}
		normalized = append(normalized, broker)
	}
	return normalized, nil
}

func validateBrokerAddress(broker string) error {
	host, port, err := net.SplitHostPort(broker)
	if err != nil {
		return fmt.Errorf("%w: broker %q must be host:port: %v", ErrInvalidConfig, broker, err)
	}
	switch host {
	case "":
		return fmt.Errorf("%w: broker %q has no host", ErrInvalidConfig, broker)
	case "0.0.0.0", "::", "*":
		return fmt.Errorf("%w: broker %q is a wildcard listen address, not a destination",
			ErrInvalidConfig, broker)
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return fmt.Errorf("%w: broker %q has an invalid port", ErrInvalidConfig, broker)
	}
	return nil
}

func normalizeTopic(topic string) (string, error) {
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return "", fmt.Errorf("%w: kafka topic is required", ErrInvalidConfig)
	}
	if strings.ContainsAny(topic, " \t\r\n") {
		return "", fmt.Errorf("%w: kafka topic %q must not contain whitespace",
			ErrInvalidConfig, topic)
	}
	switch topic {
	case ".", "..":
		return "", fmt.Errorf("%w: kafka topic %q is not a valid topic name",
			ErrInvalidConfig, topic)
	}
	return topic, nil
}
