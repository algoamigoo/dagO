package dago

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// newTestServer starts a temporary HTTP server and registers cleanup.
func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/notfound" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("Page not found"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))

	t.Cleanup(srv.Close)
	return srv
}

// HTTP ServerTests
func TestHTTPGetSuccess(t *testing.T) {
	srv := newTestServer(t)

	req := HTTPRequest{
		Client: &http.Client{},
		URL:    srv.URL,
		Method: http.MethodGet,
	}

	result, err := req.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}

	got, ok := result.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", result)
	}
	if string(got) != "OK" {
		t.Errorf("body = %q, want %q", got, "OK")
	}
}

func TestHTTPPostSuccess(t *testing.T) {
	srv := newTestServer(t)

	req := HTTPRequest{
		Client: &http.Client{},
		URL:    srv.URL,
		Method: http.MethodPost,
		Body:   bytes.NewBufferString("test body"),
	}

	result, err := req.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}

	got, ok := result.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", result)
	}
	if string(got) != "OK" {
		t.Errorf("body = %q, want %q", got, "OK")
	}
}

func TestHTTPNotFound(t *testing.T) {
	srv := newTestServer(t)

	req := HTTPRequest{
		Client: &http.Client{},
		URL:    srv.URL + "/notfound",
		Method: http.MethodGet,
	}

	_, err := req.Run(context.Background())
	if err == nil {
		t.Fatal("Run() error = nil, want non-nil for 404")
	}
}

func TestHTTPInvalidURL(t *testing.T) {
	req := HTTPRequest{
		Client: &http.Client{},
		URL:    "",
		Method: http.MethodGet,
	}

	_, err := req.Run(context.Background())
	if err == nil {
		t.Fatal("Run() error = nil, want non-nil for empty URL")
	}
}

func TestHTTPDefaults(t *testing.T) {
	srv := newTestServer(t)

	// Client, Method and Content-Type left zero-valued on purpose.
	req := HTTPRequest{URL: srv.URL}

	result, err := req.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() with defaults unexpected error: %v", err)
	}

	got, ok := result.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", result)
	}
	if string(got) != "OK" {
		t.Errorf("body = %q, want %q", got, "OK")
	}
}

// Command tests
func TestCommandSuccess(t *testing.T) {
	cmd := Command{
		Cmd:  "sh",
		Args: []string{"-c", "echo $((2 + 4))"},
	}

	result, err := cmd.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}

	got, ok := result.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", result)
	}
	if string(got) != "6\n" {
		t.Errorf("output = %q, want %q", got, "6\n")
	}
}

func TestCommandTimeout(t *testing.T) {
	cmd := Command{
		Cmd:     "sh",
		Args:    []string{"-c", "sleep 2"},
		Timeout: time.Second,
	}

	_, err := cmd.Run(context.Background())
	if err == nil {
		t.Fatal("Run() error = nil, want timeout error")
	}
}
