package pkg

import (
	"context"
	"fmt"
	client "github.com/projectriff/stream-client-go"
	"net/http"
	"strings"
)

const mimeTypeOctetStream = "application/octet-stream"

type source struct {
	server     *http.Server
	clients []*client.StreamClient
}

func NewSource(outputs []string, contentTypes []string) (*source, error) {
	clients := make([]*client.StreamClient, len(outputs))
	for i, stream := range outputs {
		if gw, topic, err := parseStreamRef(stream); err != nil {
			return nil, err
		} else if c, err := client.NewStreamClient(gw, topic, contentTypes[i]); err != nil {
			return nil, err
		} else {
			clients[i] = c
		}
	}
	s := source{clients: clients}

	m := http.NewServeMux()
	m.HandleFunc("/", s.handle)

	s.server = &http.Server{
		Addr:    ":8080",
		Handler: m,
	}
	return &s, nil
}

func parseStreamRef(output string) (string, string, error) {
	parts := strings.Split(output, "/")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("malformed stream reference: %q", output)
	}
	return parts[0], parts[1], nil
}

func (s *source) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = w.Write([]byte("Only POSTs are accepted"))
		return
	}
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = mimeTypeOctetStream
	}

	headers := make(map[string]string) // TODO: Decide which http headers to copy over based eg on WL/BL rules
	for _, c := range s.clients {
		if _, err := c.Publish(context.Background(), r.Body, nil, contentType, headers); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = fmt.Fprintf(w, "Error publishing to stream: %v", err)
			return
		}
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *source) Run(stopCh <-chan struct{}) error {
	err := s.server.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		return err
	} else {
		<-stopCh
		return nil
	}
}
