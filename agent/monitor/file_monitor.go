package monitor

import (
	"bufio"
	"context"
	"log"
	"os"
	"time"
	
	"agent/types"
)

// FileMonitor tails a log file and emits RawEvents
type FileMonitor struct {
	Path     string
	Category string
}

func (fm *FileMonitor) Start(ctx context.Context, out chan<- types.RawEvent) {
	go func() {
		file, err := os.Open(fm.Path)
		if err != nil {
			log.Printf("monitor error opening %s: %v", fm.Path, err)
			return
		}
		defer file.Close()

		// start reading from end
		file.Seek(0, os.SEEK_END)
		reader := bufio.NewReader(file)

		for {
			select {
			case <-ctx.Done():
				log.Printf("monitor stopped: %s", fm.Path)
				return
			default:
				line, err := reader.ReadString('\n')
				if err != nil {
					time.Sleep(500 * time.Millisecond)
					continue
				}

				out <- types.RawEvent{
					Timestamp: time.Now().UTC(),
					Category:  fm.Category,
					Source:    fm.Path,
					Line:      line,
				}
			}
		}
	}()
}
