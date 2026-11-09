package sender

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Client struct {
	Endpoint string
	Client   *http.Client
}

func New(endpoint string) *Client {
	return &Client{
		Endpoint: endpoint,
		Client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (c *Client) SendBatch(events []EventEnvelope) error {
	body, err := json.Marshal(events)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", c.Endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("ingest rejected batch: HTTP %d", resp.StatusCode)
	}

	return nil
}

func (c *Client) Send(batch any) error {
	events, ok := batch.([]EventEnvelope)
	if !ok {
		return fmt.Errorf("invalid batch type for HTTP sender")
	}
	return c.SendBatch(events)
}
