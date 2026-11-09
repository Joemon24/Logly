package hasher

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpointRoundtripAndReplacement(t *testing.T) {
	p := filepath.Join(t.TempDir(), "state", "hash")
	s := &State{Path: p}
	if _, err := s.LoadLastHash(); !errors.Is(err, ErrHashStateInvalid) {
		t.Fatalf("missing state: %v", err)
	}
	for _, h := range []string{"first", "second"} {
		if err := s.SaveLastHash(h); err != nil {
			t.Fatal(err)
		}
		got, err := s.LoadLastHash()
		if err != nil || got != h {
			t.Fatalf("got %q, %v", got, err)
		}
	}
	st, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0600 {
		t.Fatalf("permissions = %v", st.Mode().Perm())
	}
}

func TestCheckpointRejectsInvalidHashWithoutOverwrite(t *testing.T) {
	p := filepath.Join(t.TempDir(), "hash")
	s := &State{Path: p}
	if err := s.SaveLastHash("good"); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", "bad\nother", "bad\rother"} {
		if err := s.SaveLastHash(bad); err == nil {
			t.Fatalf("accepted %q", bad)
		}
		got, err := s.LoadLastHash()
		if err != nil || got != "good" {
			t.Fatalf("checkpoint overwritten: %q %v", got, err)
		}
	}
}
