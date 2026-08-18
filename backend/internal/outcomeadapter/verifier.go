package outcomeadapter

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const defaultMaxSVGBytes int64 = 8 << 20

var svgURLReference = regexp.MustCompile(`url\(\s*['"]?#([^\s'"\)]+)['"]?\s*\)`)

// Verifier can inspect only files under its explicit allow-list. It never
// executes commands and never exposes file bytes to the upstream request.
type Verifier struct {
	roots       []string
	workingDir  string
	maxSVGBytes int64
}

// NewVerifier creates an allow-listed local artifact verifier. Empty roots are
// valid but deliberately disable all filesystem verification.
func NewVerifier(allowedRoots []string, workingDir string) (*Verifier, error) {
	verifier := &Verifier{maxSVGBytes: defaultMaxSVGBytes}
	for _, root := range allowedRoots {
		root = strings.TrimSpace(root)
		if root == "" {
			continue
		}
		resolved, err := canonicalPath(root)
		if err != nil {
			return nil, err
		}
		verifier.roots = append(verifier.roots, resolved)
	}
	if strings.TrimSpace(workingDir) != "" {
		resolved, err := canonicalPath(workingDir)
		if err != nil {
			return nil, err
		}
		verifier.workingDir = resolved
	}
	return verifier, nil
}

// Verify returns false when the call is not a supported local file mutation or
// its path lies outside the allow-list. Callers must then leave that result
// untouched rather than infer success.
func (v *Verifier) Verify(call ToolCall) (outcomeReceipt, []string, bool) {
	path, ok := fileMutationPath(call)
	if !ok {
		return outcomeReceipt{}, nil, false
	}
	path, ok = v.resolveAllowedPath(path)
	if !ok {
		return outcomeReceipt{}, nil, false
	}

	info, err := os.Stat(path)
	exists := err == nil && info.Mode().IsRegular()
	receipt := outcomeReceipt{Artifact: &artifactReceipt{Exists: exists}}
	requirements := []string{"artifact_exists"}
	if strings.EqualFold(filepath.Ext(path), ".svg") {
		valid, referencesValid := false, false
		if exists {
			valid, referencesValid = validateSVG(path, v.maxSVGBytes)
		}
		receipt.SVG = &svgReceipt{Valid: valid, ReferencesValid: referencesValid}
		requirements = append(requirements, "svg_valid")
	}
	return receipt, requirements, true
}

func fileMutationPath(call ToolCall) (string, bool) {
	switch normalizeToolName(call.Name) {
	case "write", "edit", "multiedit", "notebookedit", "createfile", "writefile", "editfile", "replace", "patch", "applypatch":
	default:
		return "", false
	}
	var input map[string]json.RawMessage
	if json.Unmarshal(call.Input, &input) != nil {
		return "", false
	}
	for _, key := range []string{"file_path", "filePath", "path", "target"} {
		var path string
		if json.Unmarshal(input[key], &path) == nil && strings.TrimSpace(path) != "" {
			return path, true
		}
	}
	return "", false
}

func normalizeToolName(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	return strings.NewReplacer("_", "", "-", "", " ", "").Replace(value)
}

func (v *Verifier) resolveAllowedPath(value string) (string, bool) {
	if v == nil || len(v.roots) == 0 {
		return "", false
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return "", false
	}
	if !filepath.IsAbs(value) {
		if v.workingDir == "" {
			return "", false
		}
		value = filepath.Join(v.workingDir, value)
	}
	resolved, err := resolveCandidatePath(value)
	if err != nil {
		return "", false
	}
	for _, root := range v.roots {
		if isPathWithin(root, resolved) {
			return resolved, true
		}
	}
	return "", false
}

func canonicalPath(value string) (string, error) {
	abs, err := filepath.Abs(value)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", err
	}
	return filepath.Clean(resolved), nil
}

func resolveCandidatePath(value string) (string, error) {
	abs, err := filepath.Abs(value)
	if err != nil {
		return "", err
	}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		return filepath.Clean(resolved), nil
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(abs))
	if err != nil {
		return "", err
	}
	return filepath.Join(parent, filepath.Base(abs)), nil
}

func isPathWithin(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func validateSVG(path string, maxBytes int64) (bool, bool) {
	if maxBytes <= 0 {
		maxBytes = defaultMaxSVGBytes
	}
	file, err := os.Open(path)
	if err != nil {
		return false, false
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() > maxBytes {
		return false, false
	}

	decoder := xml.NewDecoder(io.LimitReader(file, maxBytes+1))
	ids := make(map[string]struct{})
	var references []string
	rootSeen := false
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return false, false
		}
		start, ok := token.(xml.StartElement)
		if !ok {
			continue
		}
		if !rootSeen {
			if !strings.EqualFold(start.Name.Local, "svg") {
				return false, false
			}
			rootSeen = true
		}
		for _, attribute := range start.Attr {
			if strings.EqualFold(attribute.Name.Local, "id") {
				id := strings.TrimSpace(attribute.Value)
				if id == "" {
					return false, false
				}
				if _, duplicate := ids[id]; duplicate {
					return false, false
				}
				ids[id] = struct{}{}
			}
			references = append(references, localSVGReferences(attribute.Value)...)
		}
	}
	if !rootSeen {
		return false, false
	}
	for _, reference := range references {
		if _, exists := ids[reference]; !exists {
			return true, false
		}
	}
	return true, true
}

func localSVGReferences(value string) []string {
	var references []string
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "#") {
		if id := strings.TrimSpace(strings.TrimPrefix(value, "#")); id != "" {
			references = append(references, id)
		}
	}
	for _, match := range svgURLReference.FindAllStringSubmatch(value, -1) {
		if len(match) > 1 && strings.TrimSpace(match[1]) != "" {
			references = append(references, strings.TrimSpace(match[1]))
		}
	}
	return references
}
