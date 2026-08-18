package gateway

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

var agentOutcomeContractKeys = []string{"agent_contract", "agentContract", "grok_agent_contract", "grokAgentContract"}

// agentOutcomeContract is intentionally limited to observable tool outcomes.
// The gateway does not infer a contract from user text or try to prove local
// filesystem state; clients opt in with request metadata instead.
type agentOutcomeContract struct {
	RequiresMutation     bool
	RequiresExecution    bool
	RequiresVerification bool
}

func (c agentOutcomeContract) hasRequirements() bool {
	return c.RequiresMutation || c.RequiresExecution || c.RequiresVerification
}

// agentOutcomeRequirementFromRequest builds the protocol-neutral guard state
// from an explicit metadata contract and the structured tool history already
// supplied by the client. A request without that contract is deliberately a
// no-op, including read-only requests with many tool calls.
func agentOutcomeRequirementFromRequest(body []byte, protocol string, stallTurns int) toolActionRequirement {
	contract, explicitContract := agentOutcomeContractFromRequest(body)
	ledger := newAgentToolLedger()
	switch protocol {
	case qualityProtocolAnthropic:
		collectAnthropicToolLedger(body, &ledger)
	case qualityProtocolChat:
		collectChatToolLedger(body, &ledger)
	default:
		collectResponsesToolLedger(body, &ledger)
	}
	ledger.finishObservationTail()
	if !explicitContract {
		// A failed Write/Edit/Patch is an unambiguous structured fact, unlike a
		// natural-language request. Retry only this narrow case even when the
		// client cannot yet supply an explicit contract.
		if ledger.failed.has(toolCapabilityMutation) {
			contract.RequiresMutation = true
		} else if ledger.observationActions >= implicitEmptyTerminalObservationThreshold(stallTurns) {
			// Native agent clients such as Claude Code cannot attach a per-turn
			// outcome contract. A terminal response with no text and no next tool
			// call after multiple completed observations is still an unambiguous
			// stalled trajectory, independent of user-language intent.
			return toolActionRequirement{
				Enabled:                    true,
				RejectEmptyTerminal:        true,
				ObservationTurns:           ledger.observationActions,
				ObservationActions:         ledger.observationActions,
				RepeatedObservationActions: ledger.observationActions,
			}
		} else {
			return toolActionRequirement{}
		}
	}
	if !contract.hasRequirements() {
		return toolActionRequirement{}
	}

	if stallTurns <= 0 {
		stallTurns = 6
	}
	return toolActionRequirement{
		Enabled:                    true,
		ContractOpen:               true,
		RequiresMutation:           contract.RequiresMutation,
		RequiresExecution:          contract.RequiresExecution,
		RequiresVerification:       contract.RequiresVerification,
		VerifiedWrite:              ledger.verified.has(toolCapabilityMutation),
		VerifiedExecution:          ledger.verified.has(toolCapabilityExecution),
		VerifiedVerification:       ledger.verified.has(toolCapabilityVerification),
		ObservationTurns:           ledger.observationActions,
		ObservationActions:         ledger.observationActions,
		RepeatedObservationActions: ledger.observationActions,
		// The next observation-only tool call is the configured threshold. A
		// productive call is always allowed through for client execution.
		StallSuspected: ledger.observationActions >= stallTurns-1,
	}
}

func implicitEmptyTerminalObservationThreshold(stallTurns int) int {
	if stallTurns <= 0 {
		stallTurns = 6
	}
	// Keep the fallback conservative even when a caller configures a smaller
	// stall threshold. One read followed by a normal answer is common.
	return max(2, stallTurns-1)
}

func agentOutcomeContractFromRequest(body []byte) (agentOutcomeContract, bool) {
	var request map[string]json.RawMessage
	if json.Unmarshal(body, &request) != nil {
		return agentOutcomeContract{}, false
	}
	if metadata, ok := rawJSONObject(request["metadata"]); ok {
		for _, key := range agentOutcomeContractKeys {
			if raw, exists := metadata[key]; exists {
				if contract, found := parseAgentOutcomeContract(raw); found {
					return contract, true
				}
			}
		}
	}
	// Top-level is useful for a controlled proxy adapter. It is not emitted by
	// standard clients, but preserving it makes the contract transport-neutral.
	for _, key := range agentOutcomeContractKeys {
		if raw, exists := request[key]; exists {
			if contract, found := parseAgentOutcomeContract(raw); found {
				return contract, true
			}
		}
	}
	return agentOutcomeContract{}, false
}

// stripAgentOutcomeMetadata removes only gateway-private contract fields from
// the upstream payload. Build rejects arbitrary metadata, while the gateway
// must retain the original body locally to evaluate the contract after the
// upstream stream completes.
func stripAgentOutcomeMetadata(body []byte) []byte {
	var request map[string]json.RawMessage
	if json.Unmarshal(body, &request) != nil {
		return body
	}
	changed := false
	if metadata, ok := rawJSONObject(request["metadata"]); ok {
		for _, key := range agentOutcomeContractKeys {
			if _, exists := metadata[key]; exists {
				delete(metadata, key)
				changed = true
			}
		}
		if changed {
			if len(metadata) == 0 {
				delete(request, "metadata")
			} else if encoded, err := json.Marshal(metadata); err == nil {
				request["metadata"] = encoded
			} else {
				return body
			}
		}
	}
	for _, key := range agentOutcomeContractKeys {
		if _, exists := request[key]; exists {
			delete(request, key)
			changed = true
		}
	}
	if !changed {
		return body
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return body
	}
	return encoded
}

func parseAgentOutcomeContract(raw json.RawMessage) (agentOutcomeContract, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return agentOutcomeContract{}, false
	}
	var encoded string
	if json.Unmarshal(raw, &encoded) == nil {
		raw = []byte(encoded)
	}
	object, ok := rawJSONObject(raw)
	if !ok {
		return agentOutcomeContract{}, false
	}

	var contract agentOutcomeContract
	found := false
	for _, key := range []string{"requires_mutation", "requiresMutation", "mutation"} {
		if value, exists := rawBool(object[key]); exists {
			contract.RequiresMutation = contract.RequiresMutation || value
			found = true
		}
	}
	for _, key := range []string{"requires_execution", "requiresExecution", "execution"} {
		if value, exists := rawBool(object[key]); exists {
			contract.RequiresExecution = contract.RequiresExecution || value
			found = true
		}
	}
	for _, key := range []string{"requires_verification", "requiresVerification", "verification"} {
		if value, exists := rawBool(object[key]); exists {
			contract.RequiresVerification = contract.RequiresVerification || value
			found = true
		}
	}
	for _, key := range []string{"requires", "required", "outcomes"} {
		values, exists := rawStringSlice(object[key])
		if !exists {
			continue
		}
		found = true
		for _, value := range values {
			switch normalizeContractOutcome(value) {
			case "mutation":
				contract.RequiresMutation = true
			case "execution":
				contract.RequiresExecution = true
			case "verification":
				contract.RequiresVerification = true
			}
		}
	}
	return contract, found
}

func normalizeContractOutcome(value string) string {
	switch normalizedToolIdentifier(value) {
	case "mutation", "mutate", "write", "edit", "patch":
		return "mutation"
	case "execution", "execute", "run", "shell", "command":
		return "execution"
	case "verification", "verify", "test", "validate", "check":
		return "verification"
	default:
		return ""
	}
}

func rawJSONObject(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	if len(raw) == 0 {
		return nil, false
	}
	var value map[string]json.RawMessage
	if json.Unmarshal(raw, &value) != nil || value == nil {
		return nil, false
	}
	return value, true
}

func rawBool(raw json.RawMessage) (bool, bool) {
	if len(raw) == 0 {
		return false, false
	}
	var value bool
	if json.Unmarshal(raw, &value) != nil {
		return false, false
	}
	return value, true
}

func rawString(raw json.RawMessage) string {
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return ""
	}
	return strings.TrimSpace(value)
}

func rawStringSlice(raw json.RawMessage) ([]string, bool) {
	if len(raw) == 0 {
		return nil, false
	}
	var values []string
	if json.Unmarshal(raw, &values) == nil {
		return values, true
	}
	var value string
	if json.Unmarshal(raw, &value) == nil {
		return []string{value}, true
	}
	return nil, false
}

type toolCapability uint8

const (
	toolCapabilityObservation toolCapability = 1 << iota
	toolCapabilityMutation
	toolCapabilityExecution
	toolCapabilityVerification
)

func (c toolCapability) has(expected toolCapability) bool {
	return c&expected != 0
}

func (c toolCapability) productive() bool {
	return c&(toolCapabilityMutation|toolCapabilityExecution|toolCapabilityVerification) != 0
}

type agentToolLedgerAction struct {
	capabilities toolCapability
	round        int
}

type agentToolLedgerRound struct {
	capabilities toolCapability
	completed    bool
}

type agentToolLedger struct {
	actions            map[string]agentToolLedgerAction
	rounds             map[int]*agentToolLedgerRound
	verified           toolCapability
	failed             toolCapability
	observationActions int
	activeRound        int
	nextRound          int
}

func newAgentToolLedger() agentToolLedger {
	return agentToolLedger{
		actions: make(map[string]agentToolLedgerAction),
		rounds:  make(map[int]*agentToolLedgerRound),
	}
}

func (l *agentToolLedger) beginRound() {
	if l == nil {
		return
	}
	l.nextRound++
	l.activeRound = l.nextRound
	l.rounds[l.activeRound] = &agentToolLedgerRound{}
}

func (l *agentToolLedger) recordCall(id string, capability toolCapability) {
	if l == nil || strings.TrimSpace(id) == "" {
		return
	}
	if l.activeRound == 0 {
		l.beginRound()
	}
	if capability == 0 {
		capability = toolCapabilityObservation
	}
	l.actions[id] = agentToolLedgerAction{capabilities: capability, round: l.activeRound}
}

func (l *agentToolLedger) recordResult(id string, success bool) {
	if l == nil || strings.TrimSpace(id) == "" {
		return
	}
	action, exists := l.actions[id]
	if !exists {
		return
	}
	if success {
		l.verified |= action.capabilities
	} else {
		l.failed |= action.capabilities
	}
	if round := l.rounds[action.round]; round != nil {
		round.capabilities |= action.capabilities
		round.completed = true
	}
}

func (l *agentToolLedger) finishObservationTail() {
	if l == nil {
		return
	}
	tail := 0
	for roundIndex := 1; roundIndex <= l.nextRound; roundIndex++ {
		round := l.rounds[roundIndex]
		if round == nil || !round.completed {
			continue
		}
		if round.capabilities.productive() {
			tail = 0
			continue
		}
		if round.capabilities.has(toolCapabilityObservation) {
			tail++
		}
	}
	l.observationActions = tail
}

func collectAnthropicToolLedger(body []byte, ledger *agentToolLedger) {
	var request struct {
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if json.Unmarshal(body, &request) != nil {
		return
	}
	for _, message := range request.Messages {
		blocks, _ := parseAnthropicHistoryContent(message.Content)
		for _, block := range blocks {
			if block.Type == "tool_use" {
				ledger.beginRound()
				break
			}
		}
		for _, block := range blocks {
			switch block.Type {
			case "tool_use":
				ledger.recordCall(block.ID, classifyToolCapability("tool_use", block.Name, block.Input))
			case "tool_result":
				ledger.recordResult(block.ToolUseID, !block.IsError)
			}
		}
	}
}

func collectResponsesToolLedger(body []byte, ledger *agentToolLedger) {
	var request struct {
		Input json.RawMessage `json:"input"`
	}
	if json.Unmarshal(body, &request) != nil || len(request.Input) == 0 {
		return
	}
	var items []json.RawMessage
	if json.Unmarshal(request.Input, &items) != nil {
		return
	}
	lastWasOutput := false
	for _, raw := range items {
		fields, ok := rawJSONObject(raw)
		if !ok {
			continue
		}
		kind := normalizedToolIdentifier(rawString(fields["type"]))
		id := rawToolCallID(fields)
		switch {
		case isResponseToolOutput(kind):
			ledger.recordResult(id, structuredToolResultSucceeded(fields))
			lastWasOutput = true
		case isResponseToolCall(kind):
			if ledger.activeRound == 0 || lastWasOutput {
				ledger.beginRound()
			}
			capability := classifyToolCapability(kind, rawString(fields["name"]), raw)
			ledger.recordCall(id, capability)
			if isSelfContainedResponseTool(kind, fields) {
				ledger.recordResult(id, structuredToolResultSucceeded(fields))
				lastWasOutput = true
			} else {
				lastWasOutput = false
			}
		}
	}
}

func collectChatToolLedger(body []byte, ledger *agentToolLedger) {
	var request struct {
		Messages []json.RawMessage `json:"messages"`
	}
	if json.Unmarshal(body, &request) != nil {
		return
	}
	for _, raw := range request.Messages {
		message, ok := rawJSONObject(raw)
		if !ok {
			continue
		}
		switch strings.ToLower(rawString(message["role"])) {
		case "assistant":
			var calls []json.RawMessage
			if json.Unmarshal(message["tool_calls"], &calls) == nil {
				if len(calls) > 0 {
					ledger.beginRound()
				}
				for _, callRaw := range calls {
					call, ok := rawJSONObject(callRaw)
					if !ok {
						continue
					}
					function, _ := rawJSONObject(call["function"])
					name := rawString(call["name"])
					if function != nil {
						name = rawString(function["name"])
					}
					ledger.recordCall(rawToolCallID(call), classifyToolCapability(rawString(call["type"]), name, callRaw))
				}
			}
		case "tool":
			ledger.recordResult(rawString(message["tool_call_id"]), structuredToolResultSucceeded(message))
		}
	}
}

func rawToolCallID(fields map[string]json.RawMessage) string {
	for _, key := range []string{"call_id", "tool_call_id", "tool_use_id", "id"} {
		if value := rawString(fields[key]); value != "" {
			return value
		}
	}
	return ""
}

func isResponseToolCall(kind string) bool {
	switch kind {
	case "functioncall", "customtoolcall", "shellcall", "localshellcall", "applypatchcall", "codeinterpretercall", "websearchcall", "computercall":
		return true
	default:
		return false
	}
}

func isResponseToolOutput(kind string) bool {
	return strings.HasSuffix(kind, "calloutput") || kind == "functioncalloutput" || kind == "customtoolcalloutput"
}

func isSelfContainedResponseTool(kind string, fields map[string]json.RawMessage) bool {
	switch kind {
	case "websearchcall", "codeinterpretercall", "computercall":
		return len(fields["status"]) > 0 || len(fields["outputs"]) > 0
	default:
		return false
	}
}

func classifyToolCapability(kind, name string, raw json.RawMessage) toolCapability {
	kind = normalizedToolIdentifier(kind)
	name = normalizedToolIdentifier(name)
	if isMutationTool(kind) || isMutationTool(name) {
		return toolCapabilityMutation
	}
	if isVerificationTool(kind) || isVerificationTool(name) {
		return toolCapabilityExecution | toolCapabilityVerification
	}
	if isShellTool(kind) || isShellTool(name) {
		return classifyShellCapability(toolCommand(raw))
	}
	if isExecutionTool(kind) || isExecutionTool(name) {
		return toolCapabilityExecution
	}
	return toolCapabilityObservation
}

func normalizedToolIdentifier(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	return strings.NewReplacer("_", "", "-", "", " ", "", ".", "", "/", "", ":", "").Replace(value)
}

func isMutationTool(value string) bool {
	switch value {
	case "write", "writefile", "create", "createfile", "edit", "editfile", "replace", "patch", "applypatch", "applypatchcall", "grok2apiapplypatch", "delete", "deletefile", "remove", "removefile", "rename", "move":
		return true
	default:
		return false
	}
}

func isExecutionTool(value string) bool {
	switch value {
	case "codeinterpreter", "codeinterpretercall", "execute", "run", "runner":
		return true
	default:
		return false
	}
}

func isVerificationTool(value string) bool {
	switch value {
	case "test", "tests", "runtests", "lint", "check", "verify", "validate":
		return true
	default:
		return false
	}
}

func isShellTool(value string) bool {
	switch value {
	case "shell", "shellcall", "localshell", "localshellcall", "bash", "terminal", "command", "shellcommand", "exec", "executecommand":
		return true
	default:
		return false
	}
}

func toolCommand(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return ""
	}
	var commands []string
	collectToolCommands(value, &commands, 0)
	return strings.Join(commands, "\n")
}

func collectToolCommands(value any, commands *[]string, depth int) {
	if depth > 6 || value == nil {
		return
	}
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			switch normalizedToolIdentifier(key) {
			case "command", "commands", "code", "script":
				collectCommandValue(child, commands, depth+1)
			case "action", "arguments", "input", "function":
				collectToolCommands(child, commands, depth+1)
			}
		}
	case string:
		trimmed := strings.TrimSpace(typed)
		if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
			var nested any
			if json.Unmarshal([]byte(trimmed), &nested) == nil {
				collectToolCommands(nested, commands, depth+1)
			}
		}
	}
}

func collectCommandValue(value any, commands *[]string, depth int) {
	switch typed := value.(type) {
	case string:
		if command := strings.TrimSpace(typed); command != "" {
			*commands = append(*commands, command)
		}
		collectToolCommands(typed, commands, depth+1)
	case []any:
		for _, child := range typed {
			collectCommandValue(child, commands, depth+1)
		}
	}
}

func classifyShellCapability(command string) toolCapability {
	command = strings.TrimSpace(command)
	if command == "" {
		return toolCapabilityObservation
	}
	if isWriteCommand(command) {
		return toolCapabilityMutation | toolCapabilityExecution
	}
	if isVerificationCommand(command) {
		return toolCapabilityExecution | toolCapabilityVerification
	}
	if isObservationCommand(command) {
		return toolCapabilityObservation
	}
	return toolCapabilityExecution
}

func isVerificationCommand(command string) bool {
	value := " " + strings.ToLower(command) + " "
	for _, marker := range []string{
		" go test ", " npm test ", " pnpm test ", " yarn test ", " bun test ", " pytest ", " cargo test ", " make test ", " mvn test ", " gradle test ",
		" golangci-lint ", " npm run lint ", " pnpm lint ", " yarn lint ", " git diff ", " git status ", " git fsck ",
	} {
		if strings.Contains(value, marker) {
			return true
		}
	}
	return false
}

func isObservationCommand(command string) bool {
	value := strings.TrimSpace(strings.ToLower(command))
	for _, prefix := range []string{"ls", "pwd", "cat", "head", "tail", "rg", "grep", "find", "stat", "git log", "git show", "git diff", "git status", "sed -n"} {
		if value == prefix || strings.HasPrefix(value, prefix+" ") {
			return true
		}
	}
	return false
}

// structuredToolResultSucceeded only trusts fields carried by the tool result
// envelope (status, is_error, exit_code and nested outcome). It never scans
// arbitrary tool text for phrases such as "success" or "failed".
func structuredToolResultSucceeded(fields map[string]json.RawMessage) bool {
	encoded, err := json.Marshal(fields)
	if err != nil {
		return false
	}
	var value any
	if json.Unmarshal(encoded, &value) != nil {
		return false
	}
	return !structuredToolResultFailed(value, 0, false)
}

func structuredToolResultFailed(value any, depth int, outcome bool) bool {
	if depth > 6 || value == nil {
		return false
	}
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			normalized := normalizedToolIdentifier(key)
			switch normalized {
			case "iserror":
				if failed, ok := child.(bool); ok && failed {
					return true
				}
			case "status":
				if status, ok := child.(string); ok && isFailureStatus(status) {
					return true
				}
			case "exitcode":
				if nonZeroNumber(child) {
					return true
				}
			case "error":
				if errorValuePresent(child) {
					return true
				}
			case "outcome":
				if structuredToolResultFailed(child, depth+1, true) {
					return true
				}
			case "output", "content":
				if structuredToolResultFailed(child, depth+1, false) {
					return true
				}
			case "type":
				if outcome {
					if label, ok := child.(string); ok && isFailureStatus(label) {
						return true
					}
				}
			}
		}
	case []any:
		for _, child := range typed {
			if structuredToolResultFailed(child, depth+1, outcome) {
				return true
			}
		}
	case string:
		trimmed := strings.TrimSpace(typed)
		if len(trimmed) == 0 || (!strings.HasPrefix(trimmed, "{") && !strings.HasPrefix(trimmed, "[")) {
			return false
		}
		var nested any
		if json.Unmarshal([]byte(trimmed), &nested) == nil {
			return structuredToolResultFailed(nested, depth+1, outcome)
		}
	}
	return false
}

func isFailureStatus(value string) bool {
	switch normalizedToolIdentifier(value) {
	case "failed", "failure", "error", "incomplete", "cancelled", "canceled", "timeout", "timedout", "aborted":
		return true
	default:
		return false
	}
}

func nonZeroNumber(value any) bool {
	switch typed := value.(type) {
	case float64:
		return typed != 0
	case json.Number:
		parsed, err := strconv.ParseInt(string(typed), 10, 64)
		return err == nil && parsed != 0
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
		return err == nil && parsed != 0
	default:
		return false
	}
}

func errorValuePresent(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case string:
		return strings.TrimSpace(typed) != ""
	case bool:
		return typed
	default:
		return true
	}
}

type qualityOutputToolAction struct {
	kind      string
	name      string
	arguments []byte
}

func outputToolIndexID(protocol string, index int) string {
	return protocol + "-" + strconv.Itoa(index)
}

func noteOutputToolCall(state *qualityScanState, id, kind, name string, arguments []byte) {
	noteOutputToolCallWithArguments(state, id, kind, name, arguments, false)
}

func noteOutputToolCallComplete(state *qualityScanState, id, kind, name string, arguments []byte) {
	noteOutputToolCallWithArguments(state, id, kind, name, arguments, true)
}

func noteOutputToolCallWithArguments(state *qualityScanState, id, kind, name string, arguments []byte, complete bool) {
	if state == nil {
		return
	}
	state.hasOutputToolUse = true
	state.hasClientContent = true
	if state.contentSeenAt.IsZero() {
		state.contentSeenAt = time.Now()
	}
	if state.outputToolCalls == nil {
		state.outputToolCalls = make(map[string]*qualityOutputToolAction)
	}
	if strings.TrimSpace(id) == "" {
		id = "tool-" + strconv.Itoa(len(state.outputToolCalls))
	}
	action := state.outputToolCalls[id]
	if action == nil {
		action = &qualityOutputToolAction{}
		state.outputToolCalls[id] = action
	}
	if strings.TrimSpace(kind) != "" {
		action.kind = kind
	}
	if strings.TrimSpace(name) != "" {
		action.name = name
	}
	if len(arguments) > 0 {
		if complete {
			action.arguments = action.arguments[:0]
		}
		if len(action.arguments) < maxToolActionTextBytes {
			remaining := maxToolActionTextBytes - len(action.arguments)
			if len(arguments) > remaining {
				arguments = arguments[:remaining]
			}
			action.arguments = append(action.arguments, arguments...)
		}
	}
	state.noteOutputToolCapability(classifyToolCapability(action.kind, action.name, action.arguments))
}

func (s *qualityScanState) noteOutputToolCapability(capability toolCapability) {
	if s == nil {
		return
	}
	if capability == 0 || capability.has(toolCapabilityObservation) {
		s.hasOutputObservation = true
	}
	if capability.has(toolCapabilityMutation) {
		s.hasOutputMutation = true
	}
	if capability.has(toolCapabilityExecution) {
		s.hasOutputExecution = true
	}
	if capability.has(toolCapabilityVerification) {
		s.hasOutputVerification = true
	}
}
