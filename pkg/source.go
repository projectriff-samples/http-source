package pkg

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	client "github.com/projectriff/stream-client-go"
)

const mimeTypeOctetStream = "application/octet-stream"

type source struct {
	server   *http.Server
	mappings map[string]*client.StreamClient
}

func NewSource(bindingsDir string, outputs []string) (*source, error) {
	m := http.NewServeMux()

	clients := make(map[string]*client.StreamClient, len(outputs))
	for _, mapping := range outputs {
		if path, binding, err := parseStreamRefMapping(mapping); err != nil {
			return nil, err
		} else if c, err := client.NewStreamClientFromBinding(filepath.Join(bindingsDir, binding)); err != nil {
			return nil, err
		} else {
			clients[path] = c
			m.HandleFunc(path, handler(c))
		}
	}
	s := source{mappings: clients}

	s.server = &http.Server{
		Addr:    ":8080",
		Handler: m,
	}
	return &s, nil
}

func parseStreamRefMapping(mapping string) (path string, binding string, err error) {
	parts := strings.SplitN(mapping, "=", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("malformed stream reference mapping, expecting <path>=<binding>: %q", mapping)
	}
	path = parts[0]
	binding = parts[1]
	return
}

func handler(client *client.StreamClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
		if _, err := client.Publish(context.Background(), r.Body, nil, contentType, headers); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = fmt.Fprintf(w, "Error publishing to stream: %v", err)
			return
		} else {
			_, _ = fmt.Printf("Successfully wrote to stream at %s/%s\n", client.Gateway, client.TopicName)
		}
		w.WriteHeader(http.StatusAccepted)
	}
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

func (s *source) Close() error {
	for _, c := range s.mappings {
		if err := c.Close(); err != nil {
			return err
		}
	}
	return nil
}
