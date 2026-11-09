package buffer

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

type DiskBuffer struct {
	path string
	mu   sync.Mutex
}

func New(path string) *DiskBuffer {
	return &DiskBuffer{path: path}
}

// Append one batch (as JSON line)
func (d *DiskBuffer) Append(v any) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	f, err := os.OpenFile(d.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	return enc.Encode(v)
}

// Read all persisted batches
func (d *DiskBuffer) ReadAll() ([][]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	f, err := os.Open(d.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var records [][]byte
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := make([]byte, len(scanner.Bytes()))
		copy(line, scanner.Bytes())
		records = append(records, line)
	}

	return records, scanner.Err()
}

// Clear buffer after successful replay
func (d *DiskBuffer) Clear() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return os.Remove(d.path)
}
