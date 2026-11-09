package hasher

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// ComputeHash hashes prev_hash + canonical(batch_payload)
func ComputeHash(prevHash string, payload any) (string, error) {
	data, err := json.Marshal(payload) // deterministic for maps w/ same keys
	if err != nil {
		return "", err
	}

	h := sha256.New()
	h.Write([]byte(prevHash))
	h.Write(data)

	return hex.EncodeToString(h.Sum(nil)), nil
}
