package retry

import (
	"log"
	"time"
)

type SenderFunc func() error

func Execute(send SenderFunc) error {
	var err error
	backoff := 500 * time.Millisecond

	for attempt := 1; attempt <= 5; attempt++ {
		err = send()
		if err == nil {
			return nil
		}

		log.Printf(
			"send failed (attempt=%d), retrying in %s: %v",
			attempt,
			backoff,
			err,
		)

		time.Sleep(backoff)
		backoff *= 2
	}

	return err
}
