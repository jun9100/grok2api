package gateway

import (
	"bytes"
	"encoding/json"
	"strings"
)

const maxToolActionTextBytes = 64 << 10

// toolActionRequirement is deliberately narrow. It is enabled only for an
// Anthropic request that advertises local write tools and asks for a file-like
// artifact, or that carries an unresolved write failure in its tool history.
// The gateway cannot inspect the client's filesystem; it verifies only the
// tool-result evidence the client supplied to this turn.
type toolActionRequirement struct {
	Enabled                bool
	ArtifactRequested      bool
	VerifiedWrite          bool
	UnresolvedWriteFailure bool
	// ContractOpen means a structured completion contract is active. It is
	// usually client-supplied metadata; a prior failed mutation may activate a
	// narrow implicit mutation-recovery contract. It is intentionally separate
	// from ArtifactRequested, whose legacy fallback uses user-language hints.
	ContractOpen               bool
	RequiresMutation           bool
	RequiresExecution          bool
	RequiresVerification       bool
	VerifiedExecution          bool
	VerifiedVerification       bool
	StallSuspected             bool
	ObservationTurns           int
	ObservationActions         int
	RepeatedObservationActions int
	// RejectEmptyTerminal is a narrow protocol-level fallback for clients that
	// cannot attach an explicit outcome contract. It only applies after a
	// completed observation-only tool tail and rejects a terminal response that
	// contains neither visible text nor another tool call.
	RejectEmptyTerminal bool
}

type toolActionHistoryAction struct {
	writeCapable bool
	target       string
}

func toolActionRequirementFromRequest(body []byte) toolActionRequirement {
	var request struct {
		Tools []struct {
			Name string `json:"name"`
		} `json:"tools"`
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if json.Unmarshal(body, &request) != nil {
		return toolActionRequirement{}
	}

	hasWritableTool := false
	for _, tool := range request.Tools {
		if isPotentialWriteTool(tool.Name) {
			hasWritableTool = true
			break
		}
	}
	if !hasWritableTool {
		return toolActionRequirement{}
	}

	actions := make(map[string]toolActionHistoryAction)
	unresolvedTargets := make(map[string]bool)
	verifiedWrite := false
	var userText strings.Builder
	for _, message := range request.Messages {
		blocks, text := parseAnthropicHistoryContent(message.Content)
		if strings.EqualFold(strings.TrimSpace(message.Role), "user") && text != "" {
			userText.WriteString(text)
			userText.WriteByte('\n')
		}
		for _, block := range blocks {
			switch block.Type {
			case "tool_use":
				if block.ID == "" {
					continue
				}
				writeCapable := toolUseWrites(block.Name, block.Input)
				actions[block.ID] = toolActionHistoryAction{
					writeCapable: writeCapable,
					target:       toolActionTarget(block.Input, block.ID),
				}
			case "tool_result":
				action, ok := actions[block.ToolUseID]
				if !ok || !action.writeCapable {
					continue
				}
				if block.IsError {
					unresolvedTargets[action.target] = true
					continue
				}
				verifiedWrite = true
				delete(unresolvedTargets, action.target)
			}
		}
	}

	requirement := toolActionRequirement{
		ArtifactRequested:      asksForArtifact(userText.String()),
		VerifiedWrite:          verifiedWrite,
		UnresolvedWriteFailure: len(unresolvedTargets) > 0,
	}
	requirement.Enabled = requirement.ArtifactRequested || requirement.UnresolvedWriteFailure
	return requirement
}

type anthropicHistoryContentBlock struct {
	Type      string          `json:"type"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	ToolUseID string          `json:"tool_use_id"`
	Input     json.RawMessage `json:"input"`
	IsError   bool            `json:"is_error"`
	Text      string          `json:"text"`
}

func parseAnthropicHistoryContent(raw json.RawMessage) ([]anthropicHistoryContentBlock, string) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, ""
	}
	var text string
	if json.Unmarshal(trimmed, &text) == nil {
		return nil, text
	}
	var blocks []anthropicHistoryContentBlock
	if json.Unmarshal(trimmed, &blocks) != nil {
		return nil, ""
	}
	var builder strings.Builder
	for _, block := range blocks {
		if block.Type == "text" && block.Text != "" {
			builder.WriteString(block.Text)
			builder.WriteByte('\n')
		}
	}
	return blocks, builder.String()
}

func isPotentialWriteTool(name string) bool {
	switch normalizeToolName(name) {
	case "write", "edit", "replace", "patch", "applypatch", "createfile", "writefile", "bash", "shell", "terminal", "execute":
		return true
	default:
		return false
	}
}

func toolUseWrites(name string, input json.RawMessage) bool {
	switch normalizeToolName(name) {
	case "write", "edit", "replace", "patch", "applypatch", "createfile", "writefile":
		return true
	case "bash", "shell", "terminal", "execute":
		var command struct {
			Command string `json:"command"`
		}
		if json.Unmarshal(input, &command) != nil {
			return false
		}
		return isWriteCommand(command.Command)
	default:
		return false
	}
}

func normalizeToolName(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.NewReplacer("_", "", "-", "", " ", "").Replace(value)
	return value
}

func isWriteCommand(command string) bool {
	value := " " + strings.ToLower(strings.TrimSpace(command)) + " "
	return strings.Contains(value, "apply_patch") ||
		strings.Contains(value, " tee ") ||
		strings.Contains(value, " touch ") ||
		strings.Contains(value, " sed -i") ||
		strings.Contains(value, " perl -pi") ||
		strings.Contains(value, " >") ||
		strings.Contains(value, ">>")
}

func toolActionTarget(input json.RawMessage, fallback string) string {
	var value struct {
		FilePath string `json:"file_path"`
		Path     string `json:"path"`
		Target   string `json:"target"`
	}
	if json.Unmarshal(input, &value) != nil {
		return fallback
	}
	for _, candidate := range []string{value.FilePath, value.Path, value.Target} {
		if candidate = strings.TrimSpace(candidate); candidate != "" {
			return candidate
		}
	}
	return fallback
}

func asksForArtifact(text string) bool {
	value := strings.ToLower(text)
	action := containsToolActionAny(value,
		"create", "write", "save", "generate", "draw", "make", "build", "implement", "edit", "modify",
		"创建", "新建", "写入", "保存", "生成", "画一个", "画一", "绘制", "作图", "制作", "实现", "编辑", "修改",
	)
	artifact := containsToolActionAny(value,
		"file", "svg", "html", "css", "json", "yaml", "yml", "markdown", "script", "code", "image", "gif",
		"文件", "代码", "脚本", "图片", "动图",
	)
	return action && artifact
}

func containsToolActionAny(value string, candidates ...string) bool {
	for _, candidate := range candidates {
		if strings.Contains(value, candidate) {
			return true
		}
	}
	return false
}

func (r toolActionRequirement) failureCode(state *qualityScanState) string {
	if !r.Enabled || state == nil || !state.terminal {
		return ""
	}
	if r.RejectEmptyTerminal && !state.terminalFailure && !state.hasOutputToolUse && state.visibleRunes == 0 {
		return ErrorAgentStall
	}
	if state.hasOutputToolUse {
		// A productive tool call must remain visible so the client can execute
		// it and return its structured result on the next request. Only an
		// observation-only continuation can prove the repeated-stall pattern.
		if r.StallSuspected && !r.outputAdvancesContract(state) && state.hasOutputObservation &&
			!state.hasOutputMutation && !state.hasOutputExecution && !state.hasOutputVerification {
			return ErrorAgentStall
		}
		return ""
	}
	if r.UnresolvedWriteFailure {
		return ErrorToolActionUnverified
	}
	if r.ContractOpen && !r.contractSatisfied() {
		return ErrorAgentContractUnfulfilled
	}
	if !r.ArtifactRequested || r.VerifiedWrite {
		return ""
	}
	text := string(state.toolActionText)
	if isManualArtifactDeferral(text) || claimsArtifactComplete(text) {
		return ErrorToolActionUnverified
	}
	return ""
}

func (r toolActionRequirement) contractSatisfied() bool {
	if r.RequiresMutation && !r.VerifiedWrite {
		return false
	}
	if r.RequiresExecution && !r.VerifiedExecution {
		return false
	}
	if r.RequiresVerification && !r.VerifiedVerification {
		return false
	}
	return true
}

func (r toolActionRequirement) outputAdvancesContract(state *qualityScanState) bool {
	if state == nil {
		return false
	}
	return (r.RequiresMutation && state.hasOutputMutation) ||
		(r.RequiresExecution && state.hasOutputExecution) ||
		(r.RequiresVerification && state.hasOutputVerification)
}

func isManualArtifactDeferral(text string) bool {
	value := strings.ToLower(text)
	if !strings.Contains(value, "```") && !strings.Contains(value, "<svg") {
		return false
	}
	return containsToolActionAny(value,
		"copy", "paste", "save as", "new file", "manually",
		"复制", "粘贴", "保存为", "新建文件", "手动", "双击",
	)
}

func claimsArtifactComplete(text string) bool {
	value := strings.ToLower(text)
	return containsToolActionAny(value,
		"i wrote", "written the file", "created the file", "saved the file", "file has been created", "file has been written",
		"已经写", "已写", "写好了", "已经创建", "已创建", "已经生成", "已生成", "已经保存", "已保存", "文件已",
	)
}
