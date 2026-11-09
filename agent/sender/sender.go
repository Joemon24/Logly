package sender

// Sender is a generic transport interface (HTTP, Kafka, etc.)
type Sender interface {
	Send(batch any) error
}


