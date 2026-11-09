package sender

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaSender struct {
	writer *kafka.Writer
}

func NewKafkaSender() *KafkaSender {
	brokers := os.Getenv("REDPANDA_BROKERS")
	if brokers == "" {
		brokers = "localhost:19092"
	}
	topic := os.Getenv("REDPANDA_TOPIC")
	if topic == "" {
		topic = "ids.events.v1"
	}
	return &KafkaSender{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(strings.Split(brokers, ",")...),
			Topic:        topic,
			RequiredAcks: kafka.RequireAll,
			Balancer:     &kafka.LeastBytes{},
		},
	}
}

func (k *KafkaSender) Send(batch any) error {
	data, err := json.Marshal(batch)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return k.writer.WriteMessages(
		ctx,
		kafka.Message{
			Value: data,
			Time:  time.Now(),
		},
	)
}

func (k *KafkaSender) Close() error {
	return k.writer.Close()
}
