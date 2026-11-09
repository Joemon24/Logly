package config

import (
	"errors"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	AgentID      string
	AgentType    string
	AgentVersion string
}

func Load() (*Config, error) {
	_ = godotenv.Load()
	cfg := &Config{
		AgentID:      os.Getenv("AGENT_ID"),
		AgentType:    os.Getenv("AGENT_TYPE"),
		AgentVersion: "0.1.0",
	}

	if cfg.AgentID == "" {
		return nil, errors.New("AGENT_ID is required")
	}
	if cfg.AgentType == "" {
		return nil, errors.New("AGENT_TYPE is required")
	}

	return cfg, nil
}

