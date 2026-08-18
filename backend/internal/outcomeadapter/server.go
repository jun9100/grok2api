package outcomeadapter

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const defaultMaxRequestBytes int64 = 32 << 20

// Config controls the local adapter. Upstream normally points to the existing
// CC Switch listener, while clients point to this adapter's listener.
type Config struct {
	UpstreamURL     string
	AllowedRoots    []string
	WorkingDir      string
	ToolCallTTL     time.Duration
	MaxRequestBytes int64
	Client          *http.Client
	OnTransform     func()
}

// Server is a transparent HTTP proxy with an opt-in Claude Messages transform.
type Server struct {
	target          *url.URL
	client          *http.Client
	store           *ToolStore
	verifier        *Verifier
	maxRequestBytes int64
	onTransform     func()
	anthropicCalls  atomic.Uint64
	trackedCalls    atomic.Uint64
	transforms      atomic.Uint64
}

// Status is safe local operational telemetry. It carries counts only: paths,
// tool inputs, artifacts, and request bodies never leave the adapter.
type Status struct {
	AnthropicRequests   uint64 `json:"anthropic_requests"`
	TrackedToolCalls    uint64 `json:"tracked_tool_calls"`
	TransformedRequests uint64 `json:"transformed_requests"`
}

// NewServer validates the target and creates the in-memory adapter state.
func NewServer(config Config) (*Server, error) {
	target, err := url.Parse(strings.TrimSpace(config.UpstreamURL))
	if err != nil {
		return nil, err
	}
	if target.Scheme != "http" && target.Scheme != "https" || target.Host == "" {
		return nil, errors.New("outcome adapter upstream URL must include http(s) scheme and host")
	}
	verifier, err := NewVerifier(config.AllowedRoots, config.WorkingDir)
	if err != nil {
		return nil, err
	}
	maxRequestBytes := config.MaxRequestBytes
	if maxRequestBytes <= 0 {
		maxRequestBytes = defaultMaxRequestBytes
	}
	client := config.Client
	if client == nil {
		client = &http.Client{Timeout: 0}
	}
	return &Server{
		target:          target,
		client:          client,
		store:           NewToolStore(config.ToolCallTTL),
		verifier:        verifier,
		maxRequestBytes: maxRequestBytes,
		onTransform:     config.OnTransform,
	}, nil
}

func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path == "/healthz" {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok\n"))
		return
	}
	if request.URL.Path == "/status" {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(s.Status())
		return
	}
	if s == nil || s.target == nil || s.client == nil {
		http.Error(writer, "outcome adapter is not configured", http.StatusServiceUnavailable)
		return
	}
	if isAnthropicMessagesRequest(request) {
		s.anthropicCalls.Add(1)
	}

	outbound, transformed, err := s.outboundRequest(request)
	if err != nil {
		http.Error(writer, "invalid local adapter request", http.StatusBadRequest)
		return
	}
	if transformed && s.onTransform != nil {
		s.transforms.Add(1)
		s.onTransform()
	} else if transformed {
		s.transforms.Add(1)
	}
	response, err := s.client.Do(outbound)
	if err != nil {
		http.Error(writer, "local upstream unavailable", http.StatusBadGateway)
		return
	}
	defer response.Body.Close()

	copyResponseHeaders(writer.Header(), response.Header)
	writer.WriteHeader(response.StatusCode)
	if isAnthropicMessagesRequest(request) && strings.Contains(strings.ToLower(response.Header.Get("Content-Type")), "text/event-stream") {
		tracker := newAnthropicStreamTracker(s.store, func() {
			s.trackedCalls.Add(1)
		})
		if err := copyTrackedStream(writer, response.Body, tracker); err == nil {
			tracker.Finish()
		}
		return
	}
	_, _ = io.Copy(writer, response.Body)
	_ = transformed
}

// Status returns a snapshot suitable for a loopback-only diagnostic endpoint.
func (s *Server) Status() Status {
	if s == nil {
		return Status{}
	}
	return Status{
		AnthropicRequests:   s.anthropicCalls.Load(),
		TrackedToolCalls:    s.trackedCalls.Load(),
		TransformedRequests: s.transforms.Load(),
	}
}

func (s *Server) outboundRequest(request *http.Request) (*http.Request, bool, error) {
	outbound := request.Clone(request.Context())
	target := *s.target
	target.Path = joinProxyPath(s.target.Path, request.URL.Path)
	target.RawQuery = request.URL.RawQuery
	outbound.URL = &target
	outbound.RequestURI = ""
	outbound.Host = s.target.Host

	if !isAnthropicMessagesRequest(request) || request.Method != http.MethodPost || request.Body == nil {
		return outbound, false, nil
	}
	if request.ContentLength > s.maxRequestBytes {
		return outbound, false, nil
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, s.maxRequestBytes+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(body)) > s.maxRequestBytes {
		return nil, false, errors.New("request body exceeds local adapter limit")
	}
	transformed, changed, err := TransformAnthropicRequest(body, s.store, s.verifier)
	if err != nil {
		return nil, false, err
	}
	outbound.Body = io.NopCloser(bytes.NewReader(transformed))
	outbound.ContentLength = int64(len(transformed))
	outbound.Header = request.Header.Clone()
	outbound.Header.Set("Content-Length", strconvFormatInt(outbound.ContentLength))
	return outbound, changed, nil
}

func isAnthropicMessagesRequest(request *http.Request) bool {
	return request != nil && strings.TrimRight(request.URL.Path, "/") == "/v1/messages"
}

func joinProxyPath(base, path string) string {
	base = strings.TrimRight(base, "/")
	path = "/" + strings.TrimLeft(path, "/")
	if base == "" || base == "/" {
		return path
	}
	return base + path
}

func copyTrackedStream(writer http.ResponseWriter, source io.Reader, tracker *anthropicStreamTracker) error {
	buffer := make([]byte, 32<<10)
	flusher, _ := writer.(http.Flusher)
	for {
		count, err := source.Read(buffer)
		if count > 0 {
			chunk := buffer[:count]
			tracker.Feed(chunk)
			if _, writeErr := writer.Write(chunk); writeErr != nil {
				return writeErr
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func copyResponseHeaders(destination, source http.Header) {
	for key, values := range source {
		switch strings.ToLower(key) {
		case "connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade":
			continue
		}
		for _, value := range values {
			destination.Add(key, value)
		}
	}
}

func strconvFormatInt(value int64) string {
	return strconv.FormatInt(value, 10)
}
