package dago

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"time"
)

// Operator defines the contract for executable tasks in a workflow.
type Operator interface {
	Run(ctx context.Context) (interface{}, error)
}

// Shell Command Operator

type Command struct {
	Cmd     string
	Args    []string
	Timeout time.Duration // Optional: defaults to 30 seconds
}

// Run executes the shell command.
// Receiver named 'c' for Command.
func (c Command) Run(ctx context.Context) (interface{}, error) {
	timeout := c.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, c.Cmd, c.Args...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("command '%s' failed: %w; stderr: %s", c.Cmd, err, stderr.String())
	}

	// Returning []byte is faster and more flexible than string
	return stdout.Bytes(), nil
}

// Unified HTTP Operator (Simplified Get & Post)

type HTTPRequest struct {
	Client      *http.Client
	URL         string
	Method      string            // Defaults to "GET" if empty
	Body        io.Reader         // Only used for POST, PUT, etc.
	Headers     map[string]string // Custom headers
	ContentType string            // Defaults to "application/json"
}

// Run executes the HTTP request.

func (h HTTPRequest) Run(ctx context.Context) (interface{}, error) {
	// 1. Set defaults
	if h.Client == nil {
		h.Client = http.DefaultClient
	}
	if h.Method == "" {
		h.Method = http.MethodGet
	}
	if h.ContentType == "" {
		h.ContentType = "application/json"
	}

	// 2. Create the request with context
	req, err := http.NewRequestWithContext(ctx, h.Method, h.URL, h.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to create %s request: %w", h.Method, err)
	}

	// 3. Set headers
	req.Header.Set("Content-Type", h.ContentType)
	for k, v := range h.Headers {
		req.Header.Set(k, v)
	}

	// 4. Execute request
	res, err := h.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute %s request: %w", h.Method, err)
	}
	defer res.Body.Close()

	// 5. Read the body
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// 6. Check for HTTP errors (e.g., 404, 500)
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return nil, fmt.Errorf("received non-2xx status %d: %s", res.StatusCode, string(body))
	}

	// Return raw bytes
	return body, nil
}
