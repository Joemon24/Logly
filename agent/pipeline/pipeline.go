package pipeline

import (
	"context"
	"log"
	"os"
	"sync/atomic"
	"time"

	"agent/batcher"
	"agent/features"
	"agent/hasher"
	"agent/monitor"
	"agent/parser"
	"agent/sender"
	"agent/types"
)

/* ================= PIPELINE ================= */

type Pipeline struct {
	monitors []*monitor.FileMonitor
}

/* ================= SEND QUEUE ITEM ================= */

type SendItem struct {
	Kind string
	Data []sender.EventEnvelope
}

/* ================= CONSTRUCTOR ================= */

func logPath(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func New() *Pipeline {
	return &Pipeline{
		monitors: []*monitor.FileMonitor{
			{Path: logPath("AUTH_LOG_PATH", "/var/log/auth.log"), Category: "auth"},
			{Path: logPath("SYSTEM_LOG_PATH", "/var/log/syslog"), Category: "system"},
			{Path: logPath("KERNEL_LOG_PATH", "/var/log/kern.log"), Category: "kernel"},
			{Path: logPath("APP_LOG_PATH", "/var/log/ids-app.log"), Category: "app"},
		},
	}
}

/* ================= START ================= */

func (p *Pipeline) Start(ctx context.Context, agentInfo types.AgentInfo) {
	log.Println("pipeline started")

	startTime := time.Now()
	heartbeatInterval := 10 * time.Second

	var (
		totalHeartbeats   uint64
		totalEventBatches uint64
		totalEvents       uint64
		kafkaHealthy      atomic.Bool
	)

	kafkaHealthy.Store(true)

	rawEvents := make(chan types.RawEvent, 200)
	enriched := make(chan any, 200)
	batches := make(chan batcher.Batch, 10)
	sendQueue := make(chan SendItem, 20)

	extractor := features.New()

	b := batcher.New(batcher.Config{
		MaxEvents:   5,
		MaxInterval: 5 * time.Second,
	})
	b.Run(ctx, enriched, batches)

	/* ---------- HASH ---------- */

	hashPath := os.Getenv("HASH_STATE_PATH")
	if hashPath == "" {
		hashPath = "./data/agent.hash"
	}

	hashState := &hasher.State{Path: hashPath}
	prevHash := ""
	trustOK := true

	if h, err := hashState.LoadLastHash(); err == nil {
		prevHash = h

	} else {

		log.Println("No previous hash state found, starting new chain")

		trustOK = true

	}

	/* ---------- SENDER ---------- */

	var outSender sender.Sender

	if os.Getenv("TRANSPORT") != "http" {
		outSender = sender.NewKafkaSender()
		log.Println("transport=kafka")
	} else {
		outSender = sender.New("http://localhost:8000/ingest")
		log.Println("transport=http")
	}

	/* ---------- RETRY WORKER ---------- */

	go func() {
		for {
			select {
			case <-ctx.Done():
				return

			case item := <-sendQueue:
				backoff := 500 * time.Millisecond

				for attempt := 1; attempt <= 5; attempt++ {
					err := outSender.Send(item.Data)

					if err == nil {
						kafkaHealthy.Store(true)

						switch item.Kind {
						case "heartbeat":
							h := atomic.AddUint64(&totalHeartbeats, 1)
							log.Printf("heartbeat delivered (total=%d)", h)

						case "events":
							b := atomic.AddUint64(&totalEventBatches, 1)
							e := atomic.AddUint64(&totalEvents, uint64(len(item.Data)))
							log.Printf("events delivered (batches=%d events=%d)", b, e)
						}

						break
					}

					kafkaHealthy.Store(false)
					log.Printf("send failed (attempt %d): %v", attempt, err)

					time.Sleep(backoff)
					backoff *= 2
				}
			}
		}
	}()

	/* ---------- HEARTBEAT ---------- */

	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				hb := []sender.EventEnvelope{
					{
						EventID:         "heartbeat",
						Timestamp:       time.Now().UTC().Format(time.RFC3339),
						IngestTimestamp: time.Now().UTC().Format(time.RFC3339),

						Agent: sender.AgentBlock{
							AgentID:      agentInfo.AgentID,
							AgentType:    agentInfo.AgentType,
							AgentVersion: agentInfo.AgentVersion,
							TrustScore:   map[bool]float64{true: 1, false: 0}[trustOK],
						},

						Event: sender.EventBlock{
							Category: "agent",
							Type:     "heartbeat",
							Action:   "alive",
							Outcome:  "success",
							Severity: "info",
						},

						RawEvent: sender.RawBlock{
							Format: "meta",
							Data:   "uptime=" + time.Since(startTime).String(),
						},

						Integrity: sender.IntegrityBlock{
							Hash:     prevHash,
							PrevHash: prevHash,
							ChainID:  agentInfo.AgentID,
						},
					},
				}

				select {
				case sendQueue <- SendItem{Kind: "heartbeat", Data: hb}:
					log.Println("heartbeat enqueued")
				default:
					log.Println("heartbeat dropped (queue full)")
				}
			}
		}
	}()

	/* ---------- MONITORS ---------- */

	for _, m := range p.monitors {
		m.Start(ctx, rawEvents)
	}

	/* ---------- PARSE ---------- */

	go func() {
		for {
			select {
			case <-ctx.Done():
				return

			case raw := <-rawEvents:
				n := parser.Parse(raw, agentInfo)
				extractor.Update(n)
				enriched <- map[string]any{"normalized": n}
			}
		}
	}()

	/* ---------- MAIN LOOP ---------- */

	for {
		select {
		case <-ctx.Done():
			log.Println("pipeline stopping")
			return

		case batch := <-batches:
			newHash, err := hasher.ComputeHash(prevHash, batch.Events)
			if err != nil {
				trustOK = false
				log.Printf("hash error: %v", err)
				continue
			}

			if err := hashState.SaveLastHash(newHash); err != nil {
				log.Printf("failed to save hash state: %v", err)
				trustOK = false
			}

			var out []sender.EventEnvelope

			for _, item := range batch.Events {
				n := item.(map[string]any)["normalized"].(types.NormalizedEvent)

				out = append(out, sender.EventEnvelope{
					EventID:         n.EventID,
					Timestamp:       n.Timestamp.UTC().Format(time.RFC3339),
					IngestTimestamp: n.IngestTimestamp.UTC().Format(time.RFC3339),

					Agent: sender.AgentBlock{
						AgentID:      agentInfo.AgentID,
						AgentType:    agentInfo.AgentType,
						AgentVersion: agentInfo.AgentVersion,
						TrustScore:   map[bool]float64{true: 1, false: 0}[trustOK],
					},

					Event: sender.EventBlock{
						Category: n.EventCategory,
						Type:     n.EventType,
						Action:   n.Action,
						Outcome:  n.Outcome,
						Severity: n.Severity,
					},

					Integrity: sender.IntegrityBlock{
						Hash:     newHash,
						PrevHash: prevHash,
						ChainID:  agentInfo.AgentID,
					},

					RawEvent: sender.RawBlock{
						Format: "text",
						Data:   n.Raw.Line,
					},
				})
			}

			sendQueue <- SendItem{Kind: "events", Data: out}
			log.Printf("events enqueued: id=%s count=%d", batch.ID, batch.Count)

			prevHash = newHash
		}
	}
}
