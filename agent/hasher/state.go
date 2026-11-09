package hasher

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var ErrHashStateInvalid = errors.New("hash state invalid or missing")

type State struct{ Path string }

func (s *State) LoadLastHash() (string, error) {
	data, err := os.ReadFile(s.Path)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrHashStateInvalid, err)
	}
	h := strings.TrimSpace(string(data))
	if h == "" {
		return "", ErrHashStateInvalid
	}
	return h, nil
}

// SaveLastHash replaces the checkpoint atomically and fsyncs it before reporting success.
func (s *State) SaveLastHash(hash string) error {
	if strings.TrimSpace(hash) == "" || strings.ContainsAny(hash, "\r\n") {
		return errors.New("invalid empty or multiline hash")
	}
	dir := filepath.Dir(s.Path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".hash-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err := f.Chmod(0600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.WriteString(hash + "\n"); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.Path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return err
	}
	return nil
}
