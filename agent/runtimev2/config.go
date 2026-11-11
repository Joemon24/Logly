// Package runtimev2 wires the authentication collector, the SQLite outbox, and
// the Kafka dispatcher into one runnable agent.
//
// It is separate from the v1 pipeline in agent/pipeline. Nothing here changes
// v1 behavior, and the v1 executable in agent/cmd/agent is untouched.
package runtimev2

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"agent/collector"
	"agent/storage"
)

const (
	// DefaultAgentVersion is stamped into events and the agent identity.
	DefaultAgentVersion = "2.0.0"
	// DefaultPollInterval is how often the collector and dispatcher poll.
	DefaultPollInterval = time.Second
	// DefaultPublishTimeout bounds one Kafka publish including broker acks.
	DefaultPublishTimeout = 10 * time.Second
	// DefaultAckTimeout bounds one SQLite acknowledgement.
	DefaultAckTimeout = 5 * time.Second
	// DefaultShutdownTimeout bounds the drain after SIGINT/SIGTERM.
	DefaultShutdownTimeout = 30 * time.Second
	// DefaultStatsInterval is how often operational counters are logged.
	DefaultStatsInterval = 30 * time.Second
)

// ErrInvalidConfig reports missing or unusable configuration.
var ErrInvalidConfig = errors.New("runtimev2: invalid config")

// ErrShutdownTimeout reports that the collector or dispatcher did not stop
// within the configured shutdown timeout.
var ErrShutdownTimeout = errors.New("runtimev2: shutdown timed out")

// Config is the complete agent configuration. Values are explicit; there are
// no broker or database defaults that could silently point at another system.
type Config struct {
	// LogPath is the authentication log to tail.
	LogPath string
	// SourceID is the stable logical identity for LogPath.
	SourceID string

	// DBPath is the SQLite outbox database.
	DBPath string

	// AgentID and AgentType identify this installation. AgentVersion is
	// informational and defaults to DefaultAgentVersion.
	AgentID      string
	AgentType    string
	AgentVersion string

	// KafkaBrokers must contain at least one explicit host:port.
	KafkaBrokers []string
	// KafkaTopic must be set explicitly.
	KafkaTopic string

	// Capacity is the maximum number of pending outbox events.
	Capacity int

	PollInterval      time.Duration
	PublishTimeout    time.Duration
	AckTimeout        time.Duration
	ShutdownTimeout   time.Duration
	StatsInterval     time.Duration
	MaxRecordsPerPoll int

	// Hostname is attached to events. Empty uses os.Hostname.
	Hostname string
}

// LoadConfig reads the LOGLY_V2_ environment. Kafka brokers and topic, the
// database path, and the agent identity are required; everything else has a
// documented default.
func LoadConfig() (Config, error) {
	cfg := Config{
		LogPath:      env("LOGLY_V2_LOG_PATH", collector.DefaultPath),
		SourceID:     env("LOGLY_V2_SOURCE_ID", collector.DefaultSourceID),
		DBPath:       os.Getenv("LOGLY_V2_DB_PATH"),
		AgentID:      os.Getenv("LOGLY_V2_AGENT_ID"),
		AgentType:    os.Getenv("LOGLY_V2_AGENT_TYPE"),
		AgentVersion: env("LOGLY_V2_AGENT_VERSION", DefaultAgentVersion),
		KafkaTopic:   os.Getenv("LOGLY_V2_KAFKA_TOPIC"),
		Hostname:     os.Getenv("LOGLY_V2_HOSTNAME"),
	}
	brokers := strings.TrimSpace(os.Getenv("LOGLY_V2_KAFKA_BROKERS"))
	if brokers != "" {
		for _, broker := range strings.Split(brokers, ",") {
			if broker = strings.TrimSpace(broker); broker != "" {
				cfg.KafkaBrokers = append(cfg.KafkaBrokers, broker)
			}
		}
	}

	var err error
	if cfg.Capacity, err = intEnvChecked("LOGLY_V2_CAPACITY", 0); err != nil {
		return Config{}, err
	}
	if cfg.MaxRecordsPerPoll, err = intEnvChecked("LOGLY_V2_MAX_RECORDS_PER_POLL", 0); err != nil {
		return Config{}, err
	}
	if cfg.PollInterval, err = durationEnv("LOGLY_V2_POLL_INTERVAL", DefaultPollInterval); err != nil {
		return Config{}, err
	}
	if cfg.PublishTimeout, err = durationEnv("LOGLY_V2_PUBLISH_TIMEOUT", DefaultPublishTimeout); err != nil {
		return Config{}, err
	}
	if cfg.AckTimeout, err = durationEnv("LOGLY_V2_ACK_TIMEOUT", DefaultAckTimeout); err != nil {
		return Config{}, err
	}
	if cfg.ShutdownTimeout, err = durationEnv("LOGLY_V2_SHUTDOWN_TIMEOUT", DefaultShutdownTimeout); err != nil {
		return Config{}, err
	}
	if cfg.StatsInterval, err = durationEnv("LOGLY_V2_STATS_INTERVAL", DefaultStatsInterval); err != nil {
		return Config{}, err
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks required values and rejects negative limits. It does not
// contact Kafka or open the database.
func (c Config) Validate() error {
	required := []struct {
		name  string
		value string
	}{
		{"LOGLY_V2_DB_PATH", c.DBPath},
		{"LOGLY_V2_AGENT_ID", c.AgentID},
		{"LOGLY_V2_AGENT_TYPE", c.AgentType},
		{"LOGLY_V2_KAFKA_TOPIC", c.KafkaTopic},
	}
	for _, field := range required {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("%w: %s is required", ErrInvalidConfig, field.name)
		}
	}
	if strings.TrimSpace(c.LogPath) == "" {
		return fmt.Errorf("%w: LOGLY_V2_LOG_PATH is required", ErrInvalidConfig)
	}
	if strings.TrimSpace(c.SourceID) == "" {
		return fmt.Errorf("%w: LOGLY_V2_SOURCE_ID is required", ErrInvalidConfig)
	}
	if len(c.KafkaBrokers) == 0 {
		return fmt.Errorf("%w: LOGLY_V2_KAFKA_BROKERS must list at least one host:port", ErrInvalidConfig)
	}
	for _, broker := range c.KafkaBrokers {
		if strings.TrimSpace(broker) == "" {
			return fmt.Errorf("%w: LOGLY_V2_KAFKA_BROKERS contains an empty entry", ErrInvalidConfig)
		}
	}
	if c.Capacity < 0 {
		return fmt.Errorf("%w: LOGLY_V2_CAPACITY must not be negative", ErrInvalidConfig)
	}
	for _, field := range []struct {
		name  string
		value time.Duration
	}{
		{"LOGLY_V2_POLL_INTERVAL", c.PollInterval},
		{"LOGLY_V2_PUBLISH_TIMEOUT", c.PublishTimeout},
		{"LOGLY_V2_ACK_TIMEOUT", c.AckTimeout},
		{"LOGLY_V2_SHUTDOWN_TIMEOUT", c.ShutdownTimeout},
		{"LOGLY_V2_STATS_INTERVAL", c.StatsInterval},
	} {
		if field.value < 0 {
			return fmt.Errorf("%w: %s must not be negative", ErrInvalidConfig, field.name)
		}
	}
	if c.MaxRecordsPerPoll < 0 {
		return fmt.Errorf("%w: LOGLY_V2_MAX_RECORDS_PER_POLL must not be negative", ErrInvalidConfig)
	}
	return nil
}

// toStorageConfig converts runtime config to the storage package contract.
func (c Config) toStorageConfig() storage.Config {
	return storage.Config{
		Path:         c.DBPath,
		AgentID:      c.AgentID,
		AgentType:    c.AgentType,
		AgentVersion: c.agentVersion(),
		Capacity:     c.Capacity,
	}
}

func (c Config) agentVersion() string {
	if c.AgentVersion == "" {
		return DefaultAgentVersion
	}
	return c.AgentVersion
}

func (c Config) pollInterval() time.Duration {
	if c.PollInterval == 0 {
		return DefaultPollInterval
	}
	return c.PollInterval
}

func (c Config) publishTimeout() time.Duration {
	if c.PublishTimeout == 0 {
		return DefaultPublishTimeout
	}
	return c.PublishTimeout
}

func (c Config) ackTimeout() time.Duration {
	if c.AckTimeout == 0 {
		return DefaultAckTimeout
	}
	return c.AckTimeout
}

func (c Config) shutdownTimeout() time.Duration {
	if c.ShutdownTimeout == 0 {
		return DefaultShutdownTimeout
	}
	return c.ShutdownTimeout
}

func (c Config) statsInterval() time.Duration {
	if c.StatsInterval == 0 {
		return DefaultStatsInterval
	}
	return c.StatsInterval
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func intEnvChecked(name string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%w: %s must be an integer", ErrInvalidConfig, name)
	}
	return value, nil
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%w: %s must be a Go duration such as 1s", ErrInvalidConfig, name)
	}
	return value, nil
}
