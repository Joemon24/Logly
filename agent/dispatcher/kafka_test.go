package dispatcher

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"agent/storage"
	"github.com/segmentio/kafka-go"
)

func TestNewKafkaProducerRequiresExplicitConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  KafkaConfig
	}{
		{name: "no brokers", cfg: KafkaConfig{Topic: "ids.events.v2"}},
		{name: "blank broker", cfg: KafkaConfig{Brokers: []string{" "}, Topic: "ids.events.v2"}},
		{name: "no port", cfg: KafkaConfig{Brokers: []string{"kafka.example"}, Topic: "ids.events.v2"}},
		{name: "wildcard host", cfg: KafkaConfig{Brokers: []string{"0.0.0.0:9092"}, Topic: "ids.events.v2"}},
		{name: "ipv6 wildcard", cfg: KafkaConfig{Brokers: []string{"[::]:9092"}, Topic: "ids.events.v2"}},
		{name: "bad port", cfg: KafkaConfig{Brokers: []string{"kafka:notaport"}, Topic: "ids.events.v2"}},
		{name: "empty topic", cfg: KafkaConfig{Brokers: []string{"kafka:9092"}}},
		{name: "whitespace topic", cfg: KafkaConfig{Brokers: []string{"kafka:9092"}, Topic: "ids events"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewKafkaProducer(test.cfg); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("NewKafkaProducer() = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestNewKafkaProducerAcceptsExplicitLoopback(t *testing.T) {
	producer, err := NewKafkaProducer(KafkaConfig{
		Brokers: []string{"127.0.0.1:19092", "[::1]:19092"},
		Topic:   "ids.events.v2",
	})
	if err != nil {
		t.Fatalf("NewKafkaProducer: %v", err)
	}
	if err := producer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestKafkaIntegration is opt-in because it needs a running broker. It never
// falls back to a default broker or topic: both must be supplied explicitly.
func TestKafkaIntegration(t *testing.T) {
	if os.Getenv("LOGLY_KAFKA_INTEGRATION") != "1" {
		t.Skip("set LOGLY_KAFKA_INTEGRATION=1 with LOGLY_KAFKA_BROKERS to run")
	}
	brokerEnv := strings.TrimSpace(os.Getenv("LOGLY_KAFKA_BROKERS"))
	if brokerEnv == "" {
		t.Skip("LOGLY_KAFKA_BROKERS must be set explicitly for the integration test")
	}
	brokers := strings.Split(brokerEnv, ",")
	topic := fmt.Sprintf("logly.dispatch.integration.%d", time.Now().UnixNano())
	createTopic(t, brokers[0], topic)

	store := openTestStore(t, testConfig(t))
	first := dispatcherEvent(t, store, "auth-log", "gen-1", 1)
	second := dispatcherEvent(t, store, "auth-log", "gen-1", 2)
	appendEvents(t, store, first, second)
	expected, err := store.Pending(context.Background(), storage.MaxPendingLimit)
	if err != nil {
		t.Fatal(err)
	}

	producer, err := NewKafkaProducer(KafkaConfig{Brokers: brokers, Topic: topic})
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	d, err := New(Config{
		Store:          store,
		Producer:       producer,
		BatchSize:      10,
		PollInterval:   50 * time.Millisecond,
		PublishTimeout: 10 * time.Second,
		AckTimeout:     5 * time.Second,
		RetryBase:      100 * time.Millisecond,
		RetryMax:       2 * time.Second,
	})
	if err != nil {
		t.Fatalf("dispatcher: %v", err)
	}
	defer d.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	acked, err := d.DispatchOnce(ctx)
	if err != nil || acked != 2 {
		t.Fatalf("dispatch = %d, %v; want 2, nil", acked, err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  1 << 20,
	})
	defer reader.Close()
	if err := reader.SetOffset(kafka.FirstOffset); err != nil {
		t.Fatalf("seek to first offset: %v", err)
	}

	for i, entry := range expected {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("read message %d: %v", i, err)
		}
		if string(message.Key) != string(DeliveryKey(entry)) {
			t.Fatalf("message %d key = %q, want %q", i, message.Key, DeliveryKey(entry))
		}
		if !bytes.Equal(message.Value, entry.Payload) {
			t.Fatalf("message %d payload differs from the stored bytes", i)
		}
	}
	if entries, err := store.Pending(context.Background(), storage.MaxPendingLimit); err != nil || len(entries) != 0 {
		t.Fatalf("outbox after integration dispatch = %d, err %v", len(entries), err)
	}
}

func createTopic(t *testing.T, broker, topic string) {
	t.Helper()
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		t.Fatalf("dial broker %s: %v", broker, err)
	}
	controller, err := conn.Controller()
	_ = conn.Close()
	if err != nil {
		t.Fatalf("find controller: %v", err)
	}
	controllerConn, err := kafka.Dial("tcp",
		net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Fatalf("dial controller: %v", err)
	}
	defer controllerConn.Close()
	if err := controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
}
