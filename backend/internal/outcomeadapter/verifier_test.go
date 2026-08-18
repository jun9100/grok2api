package outcomeadapter

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestVerifierVerifiesAllowedSVGArtifact(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "pelican.svg")
	writeTestFile(t, path, `<svg xmlns="http://www.w3.org/2000/svg"><defs><linearGradient id="sky"/></defs><rect fill="url(#sky)"/></svg>`)

	verifier, err := NewVerifier([]string{root}, root)
	if err != nil {
		t.Fatal(err)
	}
	receipt, requirements, enforced := verifier.Verify(ToolCall{
		ID:    "toolu_svg",
		Name:  "Write",
		Input: json.RawMessage(`{"file_path":"pelican.svg"}`),
	})
	if !enforced {
		t.Fatal("allowed Write was not enforced")
	}
	if receipt.Artifact == nil || !receipt.Artifact.Exists {
		t.Fatalf("artifact receipt = %#v", receipt.Artifact)
	}
	if receipt.SVG == nil || !receipt.SVG.Valid || !receipt.SVG.ReferencesValid {
		t.Fatalf("SVG receipt = %#v", receipt.SVG)
	}
	if want := []string{"artifact_exists", "svg_valid"}; !reflect.DeepEqual(requirements, want) {
		t.Fatalf("requirements = %v, want %v", requirements, want)
	}
}

func TestVerifierReportsBrokenSVGReferences(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "broken.svg")
	writeTestFile(t, path, `<svg xmlns="http://www.w3.org/2000/svg"><use href="#missing"/></svg>`)

	verifier, err := NewVerifier([]string{root}, root)
	if err != nil {
		t.Fatal(err)
	}
	receipt, requirements, enforced := verifier.Verify(ToolCall{
		Name:  "Write",
		Input: json.RawMessage(`{"file_path":"broken.svg"}`),
	})
	if !enforced || receipt.Artifact == nil || !receipt.Artifact.Exists || receipt.SVG == nil || !receipt.SVG.Valid || receipt.SVG.ReferencesValid {
		t.Fatalf("broken SVG result = receipt=%#v requirements=%v enforced=%t", receipt, requirements, enforced)
	}
}

func TestVerifierRejectsPathsOutsideAllowList(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	outsidePath := filepath.Join(outside, "outside.svg")
	writeTestFile(t, outsidePath, `<svg xmlns="http://www.w3.org/2000/svg"/>`)

	verifier, err := NewVerifier([]string{root}, root)
	if err != nil {
		t.Fatal(err)
	}
	for name, input := range map[string]json.RawMessage{
		"outside absolute path": json.RawMessage(`{"file_path":` + quoteJSON(outsidePath) + `}`),
		"non-file tool":         json.RawMessage(`{"file_path":"inside.svg"}`),
	} {
		t.Run(name, func(t *testing.T) {
			toolName := "Write"
			if name == "non-file tool" {
				toolName = "Bash"
			}
			if receipt, requirements, enforced := verifier.Verify(ToolCall{Name: toolName, Input: input}); enforced || receipt.Artifact != nil || len(requirements) != 0 {
				t.Fatalf("outside/unrelated call was enforced: receipt=%#v requirements=%v", receipt, requirements)
			}
		})
	}

	link := filepath.Join(root, "linked.svg")
	if err := os.Symlink(outsidePath, link); err != nil {
		t.Fatal(err)
	}
	if receipt, requirements, enforced := verifier.Verify(ToolCall{Name: "Write", Input: json.RawMessage(`{"file_path":"linked.svg"}`)}); enforced || receipt.Artifact != nil || len(requirements) != 0 {
		t.Fatalf("symlink escape was enforced: receipt=%#v requirements=%v", receipt, requirements)
	}
}

func writeTestFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

func quoteJSON(value string) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}
