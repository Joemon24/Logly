package sender

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

type statusTransport int

func (status statusTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: int(status), Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
}
func TestHTTPStatusAcknowledgement(t *testing.T) {
	for _, code := range []int{200, 202, 400, 401, 500, 503} {
		client := New("http://example.invalid/ingest")
		client.Client.Transport = statusTransport(code)
		err := client.SendBatch([]EventEnvelope{})
		if (err == nil) != (code >= 200 && code < 300) {
			t.Fatalf("status %d: error %v", code, err)
		}
	}
}
