package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/chenyme/grok2api/backend/internal/domain/audit"
)

const (
	qualityProtocolChat        = "chat"
	qualityProtocolResponses   = "responses"
	qualityProtocolAnthropic   = "anthropic"
	qualityReasoningSSEComment = ": grok2api-reasoning-start"
	qualityHoldMaxBufferBytes  = 4 << 20
)

type qualityScanState struct {
	protocol              string
	pending               []byte
	hasThinking           bool
	visibleRunes          int
	reasoningTokens       int64
	outputTokens          int64
	usage                 Usage
	responseID            string
	terminal              bool
	terminalFailure       bool
	hasClientContent      bool
	contentSeenAt         time.Time
	toolActionText        []byte
	hasOutputToolUse      bool
	hasOutputMutation     bool
	hasOutputExecution    bool
	hasOutputVerification bool
	hasOutputObservation  bool
	outputToolCalls       map[string]*qualityOutputToolAction
}

func qualityProtocolForOperation(operation audit.Operation) string {
	switch operation {
	case audit.OperationChat:
		return qualityProtocolChat
	case audit.OperationMessages:
		return qualityProtocolAnthropic
	default:
		return qualityProtocolResponses
	}
}

func (s *qualityScanState) signals() QualityStreamSignals {
	visible := int64((s.visibleRunes + 3) / 4)
	if s.usage.Reported {
		fromUsage := s.usage.OutputTokens - s.usage.ReasoningTokens
		if fromUsage > visible {
			visible = fromUsage
		}
	}
	output := s.outputTokens
	if s.usage.Reported && s.usage.OutputTokens > output {
		output = s.usage.OutputTokens
	}
	if output < visible {
		output = visible
	}
	return QualityStreamSignals{
		HasThinking:     s.hasThinking || s.reasoningTokens > 0 || s.usage.ReasoningTokens > 0,
		HasToolUse:      s.hasOutputToolUse,
		VisibleTokens:   visible,
		ReasoningTokens: max(s.reasoningTokens, s.usage.ReasoningTokens),
		OutputTokens:    output,
		Terminal:        s.terminal,
	}
}

// auditUsage preserves upstream usage when present and otherwise turns held
// visible output into a conservative token estimate. A degraded stream may
// end before its usage frame, but it still needs usable account/IP evidence.
func (s *qualityScanState) auditUsage() Usage {
	usage := s.usage
	signals := s.signals()
	if usage.OutputTokens < signals.OutputTokens {
		usage.OutputTokens = signals.OutputTokens
	}
	if usage.ReasoningTokens < signals.ReasoningTokens {
		usage.ReasoningTokens = signals.ReasoningTokens
	}
	if usage.TotalTokens < usage.InputTokens+usage.OutputTokens {
		usage.TotalTokens = usage.InputTokens + usage.OutputTokens
	}
	return usage
}

// ObserveQualityChunk feeds one SSE chunk into the hold classifier state.
// This is the shipped scanner used by peekQualityStream.
func ObserveQualityChunk(state *qualityScanState, chunk []byte) {
	if state == nil || len(chunk) == 0 {
		return
	}
	state.pending = append(state.pending, chunk...)
	for {
		index := bytes.IndexByte(state.pending, '\n')
		if index < 0 {
			if len(state.pending) > 1<<20 {
				state.pending = nil
			}
			return
		}
		line := bytes.TrimSpace(state.pending[:index])
		state.pending = state.pending[index+1:]
		if len(line) == 0 {
			continue
		}
		if bytes.Equal(line, []byte(qualityReasoningSSEComment)) {
			state.hasThinking = true
			continue
		}
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		payload := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
		if bytes.Equal(payload, []byte("[DONE]")) {
			state.terminal = true
			continue
		}
		observeQualityPayload(state, payload)
	}
}

func observeQualityPayload(state *qualityScanState, payload []byte) {
	switch state.protocol {
	case qualityProtocolChat:
		observeQualityChat(state, payload)
	case qualityProtocolAnthropic:
		observeQualityAnthropic(state, payload)
	default:
		observeQualityResponses(state, payload)
	}
}

func firstOutputToolID(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func qualityToolArgumentValue(values ...json.RawMessage) []byte {
	for _, raw := range values {
		trimmed := bytes.TrimSpace(raw)
		if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) || bytes.Equal(trimmed, []byte(`""`)) {
			continue
		}
		return append([]byte(nil), trimmed...)
	}
	return nil
}

func observeQualityChat(state *qualityScanState, payload []byte) {
	var event struct {
		ID      string          `json:"id"`
		Model   string          `json:"model"`
		Type    string          `json:"type"`
		Error   json.RawMessage `json:"error"`
		Choices []struct {
			Delta struct {
				Content          string `json:"content"`
				Reasoning        string `json:"reasoning"`
				ReasoningContent string `json:"reasoning_content"`
				ThinkingContent  string `json:"thinking_content"`
				ToolCalls        []struct {
					Index    int    `json:"index"`
					ID       string `json:"id"`
					Type     string `json:"type"`
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"delta"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage *struct {
			PromptTokens            int64 `json:"prompt_tokens"`
			CompletionTokens        int64 `json:"completion_tokens"`
			TotalTokens             int64 `json:"total_tokens"`
			CompletionTokensDetails struct {
				ReasoningTokens int64 `json:"reasoning_tokens"`
			} `json:"completion_tokens_details"`
		} `json:"usage"`
	}
	if json.Unmarshal(payload, &event) != nil {
		return
	}
	if state.responseID == "" {
		state.responseID = event.ID
	}
	if event.Type == "error" || (len(event.Error) > 0 && !bytes.Equal(bytes.TrimSpace(event.Error), []byte("null"))) {
		state.terminal = true
		state.terminalFailure = true
	}
	if event.Usage != nil {
		state.usage.Reported = true
		state.usage.InputTokens = event.Usage.PromptTokens
		state.usage.OutputTokens = event.Usage.CompletionTokens
		state.usage.ReasoningTokens = event.Usage.CompletionTokensDetails.ReasoningTokens
		state.usage.TotalTokens = event.Usage.TotalTokens
		state.usage.ResponseModel = event.Model
		state.outputTokens = event.Usage.CompletionTokens
		state.reasoningTokens = event.Usage.CompletionTokensDetails.ReasoningTokens
		if state.reasoningTokens > 0 {
			state.hasThinking = true
			state.hasClientContent = true
		}
	}
	for _, choice := range event.Choices {
		delta := choice.Delta
		if delta.Reasoning != "" || delta.ReasoningContent != "" || delta.ThinkingContent != "" {
			state.hasThinking = true
			state.hasClientContent = true
		}
		if delta.Content != "" {
			noteVisibleContent(state, delta.Content)
		}
		for _, call := range delta.ToolCalls {
			noteOutputToolCall(
				state,
				outputToolIndexID("chat", call.Index),
				call.Type,
				call.Function.Name,
				[]byte(call.Function.Arguments),
			)
		}
		if choice.FinishReason != "" {
			state.terminal = true
		}
	}
}

func observeQualityResponses(state *qualityScanState, payload []byte) {
	var event struct {
		Type  string `json:"type"`
		Delta string `json:"delta"`
		Item  struct {
			ID        string          `json:"id"`
			CallID    string          `json:"call_id"`
			Type      string          `json:"type"`
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
			Action    json.RawMessage `json:"action"`
			Input     json.RawMessage `json:"input"`
		} `json:"item"`
		ItemID    string          `json:"item_id"`
		CallID    string          `json:"call_id"`
		Arguments json.RawMessage `json:"arguments"`
		Response  *struct {
			ID    string `json:"id"`
			Model string `json:"model"`
			Usage *struct {
				OutputTokens        int64 `json:"output_tokens"`
				InputTokens         int64 `json:"input_tokens"`
				TotalTokens         int64 `json:"total_tokens"`
				OutputTokensDetails struct {
					ReasoningTokens int64 `json:"reasoning_tokens"`
				} `json:"output_tokens_details"`
			} `json:"usage"`
		} `json:"response"`
	}
	if json.Unmarshal(payload, &event) != nil {
		return
	}
	switch event.Type {
	case "response.completed":
		state.terminal = true
	case "response.incomplete", "response.failed", "response.error", "error":
		state.terminal = true
		state.terminalFailure = true
	case "response.reasoning_text.delta", "response.reasoning_summary_text.delta":
		if event.Delta != "" {
			state.hasThinking = true
			state.hasClientContent = true
		}
	case "response.output_item.added":
		if event.Item.Type == "reasoning" && event.Item.ID != "" {
			state.hasThinking = true
		}
		if isResponseToolCall(normalizedToolIdentifier(event.Item.Type)) {
			noteOutputToolCall(
				state,
				firstOutputToolID(event.Item.ID, event.Item.CallID, event.ItemID, event.CallID),
				event.Item.Type,
				event.Item.Name,
				qualityToolArgumentValue(event.Item.Action, event.Item.Arguments, event.Item.Input),
			)
		}
	case "response.output_item.done":
		if isResponseToolCall(normalizedToolIdentifier(event.Item.Type)) {
			noteOutputToolCallComplete(
				state,
				firstOutputToolID(event.Item.ID, event.Item.CallID, event.ItemID, event.CallID),
				event.Item.Type,
				event.Item.Name,
				qualityToolArgumentValue(event.Item.Action, event.Item.Arguments, event.Item.Input),
			)
		}
	case "response.output_text.delta":
		if event.Delta != "" {
			noteVisibleContent(state, event.Delta)
		}
	case "response.function_call_arguments.delta", "response.custom_tool_call_input.delta":
		if event.Delta != "" {
			noteOutputToolCall(state, firstOutputToolID(event.ItemID, event.CallID), "", "", []byte(event.Delta))
		}
	case "response.function_call_arguments.done", "response.custom_tool_call_input.done":
		if arguments := qualityToolArgumentValue(event.Arguments); len(arguments) > 0 {
			noteOutputToolCallComplete(state, firstOutputToolID(event.ItemID, event.CallID), "", "", arguments)
		}
	}
	if event.Response != nil {
		if state.responseID == "" {
			state.responseID = event.Response.ID
		}
		if event.Response.Usage != nil {
			state.usage.Reported = true
			state.usage.InputTokens = event.Response.Usage.InputTokens
			state.usage.OutputTokens = event.Response.Usage.OutputTokens
			state.usage.ReasoningTokens = event.Response.Usage.OutputTokensDetails.ReasoningTokens
			state.usage.TotalTokens = event.Response.Usage.TotalTokens
			state.usage.ResponseModel = event.Response.Model
			state.outputTokens = event.Response.Usage.OutputTokens
			state.reasoningTokens = event.Response.Usage.OutputTokensDetails.ReasoningTokens
			if state.reasoningTokens > 0 {
				state.hasThinking = true
				state.hasClientContent = true
			}
		}
	}
}

func observeQualityAnthropic(state *qualityScanState, payload []byte) {
	var event struct {
		Type         string `json:"type"`
		Index        int    `json:"index"`
		ContentBlock struct {
			ID    string          `json:"id"`
			Type  string          `json:"type"`
			Name  string          `json:"name"`
			Input json.RawMessage `json:"input"`
		} `json:"content_block"`
		Delta struct {
			Type        string `json:"type"`
			Text        string `json:"text"`
			Thinking    string `json:"thinking"`
			PartialJSON string `json:"partial_json"`
		} `json:"delta"`
		Usage *struct {
			OutputTokens        int64 `json:"output_tokens"`
			OutputTokensDetails struct {
				ThinkingTokens int64 `json:"thinking_tokens"`
			} `json:"output_tokens_details"`
		} `json:"usage"`
	}
	if json.Unmarshal(payload, &event) != nil {
		return
	}
	switch event.Type {
	case "message_stop":
		state.terminal = true
	case "error":
		state.terminal = true
		state.terminalFailure = true
	case "content_block_start":
		if event.ContentBlock.Type == "thinking" {
			state.hasThinking = true
		}
		if event.ContentBlock.Type == "tool_use" {
			noteOutputToolCall(
				state,
				outputToolIndexID("anthropic", event.Index),
				event.ContentBlock.Type,
				event.ContentBlock.Name,
				event.ContentBlock.Input,
			)
		}
	case "content_block_delta":
		if event.Delta.Type == "thinking_delta" && event.Delta.Thinking != "" {
			state.hasThinking = true
			state.hasClientContent = true
		}
		if event.Delta.Type == "text_delta" && event.Delta.Text != "" {
			noteVisibleContent(state, event.Delta.Text)
		}
		if event.Delta.Type == "input_json_delta" && event.Delta.PartialJSON != "" {
			noteOutputToolCall(state, outputToolIndexID("anthropic", event.Index), "", "", []byte(event.Delta.PartialJSON))
		}
	}
	if event.Usage != nil {
		state.usage.Reported = true
		state.usage.OutputTokens = event.Usage.OutputTokens
		state.usage.ReasoningTokens = event.Usage.OutputTokensDetails.ThinkingTokens
		state.outputTokens = event.Usage.OutputTokens
		state.reasoningTokens = event.Usage.OutputTokensDetails.ThinkingTokens
		if state.reasoningTokens > 0 {
			state.hasThinking = true
			state.hasClientContent = true
		}
	}
}

func noteVisibleContent(state *qualityScanState, text string) {
	if text == "" {
		return
	}
	state.visibleRunes += utf8.RuneCountInString(text)
	if remaining := maxToolActionTextBytes - len(state.toolActionText); remaining > 0 {
		if len(text) > remaining {
			text = text[:remaining]
		}
		state.toolActionText = append(state.toolActionText, text...)
	}
	state.hasClientContent = true
	if state.contentSeenAt.IsZero() {
		state.contentSeenAt = time.Now()
	}
}

// peekZeroTokenStream keeps a Build stream private until it carries model
// output. The returned prefix is replayed unchanged once output is present;
// a non-nil error therefore always means no bytes have been exposed to the
// downstream client yet.
func peekZeroTokenStream(ctx context.Context, body io.ReadCloser, protocol string) (io.ReadCloser, error) {
	if body == nil {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = body.Close()
		case <-stop:
		}
	}()

	state := qualityScanState{protocol: protocol}
	var held bytes.Buffer
	buf := make([]byte, 4096)
	for {
		if ctx.Err() != nil {
			_ = body.Close()
			return io.NopCloser(bytes.NewReader(held.Bytes())), ctx.Err()
		}
		n, readErr := body.Read(buf)
		if n > 0 {
			if held.Len()+n > qualityHoldMaxBufferBytes {
				_, _ = held.Write(buf[:n])
				return newPrefixReplay(&held, body), nil
			}
			_, _ = held.Write(buf[:n])
			ObserveQualityChunk(&state, buf[:n])
			if state.hasClientContent || state.terminal {
				return newPrefixReplay(&held, body), nil
			}
		}
		if readErr == nil {
			continue
		}
		_ = body.Close()
		if ctx.Err() != nil {
			return io.NopCloser(bytes.NewReader(held.Bytes())), ctx.Err()
		}
		if readErr == io.EOF {
			return io.NopCloser(bytes.NewReader(held.Bytes())), errZeroTokenStreamIncomplete
		}
		return io.NopCloser(bytes.NewReader(held.Bytes())), readErr
	}
}

func qualityHoldOutcome(state *qualityScanState, cfg QualityRetryRuntime, toolAction toolActionRequirement) (QualityVerdict, string) {
	if code := toolAction.failureCode(state); code != "" {
		return QualityWithhold, code
	}
	verdict := ClassifyQualityHold(state.signals(), cfg.MinOutputTokens)
	if verdict == QualityWithhold {
		return verdict, ErrorQualityDegraded
	}
	return verdict, ""
}

func peekQualityStream(ctx context.Context, body io.ReadCloser, protocol string, cfg QualityRetryRuntime, toolAction toolActionRequirement) (io.ReadCloser, QualityVerdict, string, Usage, string, error) {
	cfg = normalizeQualityRetry(cfg)
	if body == nil {
		return io.NopCloser(bytes.NewReader(nil)), QualityDeliver, "", Usage{}, "", nil
	}
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = body.Close()
		case <-stop:
		}
	}()

	state := qualityScanState{protocol: protocol}
	var held bytes.Buffer
	buf := make([]byte, 4096)
	for {
		if ctx.Err() != nil {
			_ = body.Close()
			return io.NopCloser(bytes.NewReader(held.Bytes())), QualityDeliver, "", state.auditUsage(), state.responseID, ctx.Err()
		}
		if !toolAction.Enabled && !state.contentSeenAt.IsZero() && cfg.HoldTimeout > 0 && time.Since(state.contentSeenAt) >= cfg.HoldTimeout {
			sig := state.signals()
			sig.HoldExpired = true
			verdict := ClassifyQualityHold(sig, cfg.MinOutputTokens)
			code := ""
			if verdict == QualityWithhold {
				code = ErrorQualityDegraded
			}
			return newPrefixReplay(&held, body), verdict, code, state.auditUsage(), state.responseID, nil
		}
		if toolAction.Enabled {
			if state.terminal {
				verdict, code := qualityHoldOutcome(&state, cfg, toolAction)
				return newPrefixReplay(&held, body), verdict, code, state.auditUsage(), state.responseID, nil
			}
		} else if verdict, code := qualityHoldOutcome(&state, cfg, toolAction); verdict != QualityWait {
			return newPrefixReplay(&held, body), verdict, code, state.auditUsage(), state.responseID, nil
		}
		n, err := body.Read(buf)
		if n > 0 {
			if held.Len()+n > qualityHoldMaxBufferBytes {
				_, _ = held.Write(buf[:n])
				return newPrefixReplay(&held, body), QualityDeliver, "", state.auditUsage(), state.responseID, nil
			}
			_, _ = held.Write(buf[:n])
			ObserveQualityChunk(&state, buf[:n])
		}
		if err == io.EOF {
			state.terminal = true
			verdict, code := qualityHoldOutcome(&state, cfg, toolAction)
			return newPrefixReplay(&held, io.NopCloser(bytes.NewReader(nil))), verdict, code, state.auditUsage(), state.responseID, nil
		}
		if err != nil {
			_ = body.Close()
			return io.NopCloser(bytes.NewReader(held.Bytes())), QualityDeliver, "", state.auditUsage(), state.responseID, err
		}
	}
}

func newPrefixReplay(held *bytes.Buffer, rest io.ReadCloser) io.ReadCloser {
	if rest == nil {
		rest = io.NopCloser(bytes.NewReader(nil))
	}
	if held == nil || held.Len() == 0 {
		return rest
	}
	return &replayReadCloser{Reader: io.MultiReader(bytes.NewReader(held.Bytes()), rest), source: rest}
}
