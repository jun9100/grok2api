package outcomeadapter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestServerForwardsTransformedAnthropicRequest(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, filepath.Join(root, "result.txt"), "verified")

	var received []byte
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/messages" || request.URL.Query().Get("trace") != "1" {
			http.Error(writer, "wrong path", http.StatusBadRequest)
			return
		}
		var err error
		received, err = io.ReadAll(request.Body)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	transformCount := 0
	adapter, err := NewServer(Config{
		UpstreamURL:  upstream.URL,
		AllowedRoots: []string{root},
		WorkingDir:   root,
		OnTransform: func() {
			transformCount++
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter.store.Put(ToolCall{ID: "toolu_write", Name: "Write", Input: json.RawMessage(`{"file_path":"result.txt"}`)})
	proxy := httptest.NewServer(adapter)
	defer proxy.Close()

	request, err := http.NewRequest(http.MethodPost, proxy.URL+"/v1/messages?trace=1", bytes.NewReader([]byte(`{"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_write","content":"done"}]}]}`)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-API-Key", "preserved")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("status = %d: %s", response.StatusCode, body)
	}
	if !bytes.Contains(received, []byte(`"grok_agent_contract"`)) || !bytes.Contains(received, []byte(`"artifact_exists"`)) {
		t.Fatalf("upstream did not receive adapter contract: %s", received)
	}
	if transformCount != 1 {
		t.Fatalf("transform callback count = %d, want 1", transformCount)
	}
}

func TestServerTracksAnthropicSSEToolUse(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := writer.(http.Flusher)
		_, _ = fmt.Fprint(writer, "data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"id\":\"toolu_stream\",\"type\":\"tool_use\",\"name\":\"Write\",\"input\":{}}}\n")
		flusher.Flush()
		_, _ = fmt.Fprint(writer, `data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"file_path\":\"stream.svg\"}"}}`)
	}))
	defer upstream.Close()

	adapter, err := NewServer(Config{UpstreamURL: upstream.URL})
	if err != nil {
		t.Fatal(err)
	}
	proxy := httptest.NewServer(adapter)
	defer proxy.Close()

	request, err := http.NewRequest(http.MethodPost, proxy.URL+"/v1/messages", bytes.NewReader([]byte(`{"stream":true,"messages":[]}`)))
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()

	call, ok := adapter.store.Get("toolu_stream")
	if !ok || call.Name != "Write" || string(call.Input) != `{"file_path":"stream.svg"}` {
		t.Fatalf("SSE tool call = %#v, found=%t", call, ok)
	}
	if status := adapter.Status(); status.AnthropicRequests != 1 || status.TrackedToolCalls != 1 || status.TransformedRequests != 0 {
		t.Fatalf("adapter status = %#v", status)
	}
}
