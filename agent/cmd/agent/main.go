package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	
	"agent/config"
	"agent/pipeline"
	"agent/types"
)

func main() {
	log.Println("agent starting")

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	log.Printf("agent_id=%s agent_type=%s version=%s",
		cfg.AgentID, cfg.AgentType, cfg.AgentVersion)

	// Build AgentInfo ONCE (correct ownership)
	agentInfo := types.AgentInfo{
		AgentID:      cfg.AgentID,
		AgentType:    cfg.AgentType,
		AgentVersion: cfg.AgentVersion,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Printf("shutdown signal received: %v", sig)
		cancel()
	}()

	p := pipeline.New()
	p.Start(ctx, agentInfo)

	log.Println("agent exited cleanly")
}
