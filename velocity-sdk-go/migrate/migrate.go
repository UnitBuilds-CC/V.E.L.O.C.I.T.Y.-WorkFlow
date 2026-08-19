// Package migrate provides migration tooling for converting Temporal, Restate,
// and DBOS workflows to Velocity Go SDK workflows.
//
// Usage:
//
//	go run sdk/go/migrate/main.go --src ./my_project --from temporal --to velocity
//	go run sdk/go/migrate/main.go --src ./workflows --from auto --to velocity
//	go run sdk/go/migrate/main.go --detect ./my_project
package migrate

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// ─── Pattern Definition ──────────────────────────────────────────────────────

// MigrationPattern defines a source pattern and its Velocity replacement.
type MigrationPattern struct {
	Name            string
	SourcePattern   *regexp.Regexp
	TargetTemplate  string
	SourceFramework string // "temporal", "restate", "dbos", or "any"
}

// ─── Temporal → Velocity Patterns ────────────────────────────────────────────

var TemporalPatterns = []*MigrationPattern{
	// Import replacements
	{
		Name:           "temporal-import-workflow",
		SourcePattern:  regexp.MustCompile(`"go\.temporal\.io/sdk/workflow"`),
		TargetTemplate: `"github.com/velocity-workflow/velocity-sdk-go"`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-import-activity",
		SourcePattern:  regexp.MustCompile(`"go\.temporal\.io/sdk/activity"`),
		TargetTemplate: `"github.com/velocity-workflow/velocity-sdk-go"`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-import-client",
		SourcePattern:  regexp.MustCompile(`"go\.temporal\.io/sdk/client"`),
		TargetTemplate: `"github.com/velocity-workflow/velocity-sdk-go"`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-import-worker",
		SourcePattern:  regexp.MustCompile(`"go\.temporal\.io/sdk/worker"`),
		TargetTemplate: `"github.com/velocity-workflow/velocity-sdk-go"`,
		SourceFramework: "temporal",
	},
	// Function signature replacements
	{
		Name:           "temporal-workflow-func",
		SourcePattern:  regexp.MustCompile(`func\s+(\w+)\s*\(ctx\s+workflow\.Context\)`),
		TargetTemplate: `func $1(ctx *velocity.WorkflowContext)`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-activity-func",
		SourcePattern:  regexp.MustCompile(`func\s+(\w+)\s*\(ctx\s+activity\.Context`),
		TargetTemplate: `func $1(ctx *velocity.ActivityContext`,
		SourceFramework: "temporal",
	},
	// Method call replacements
	{
		Name:           "temporal-execute-activity",
		SourcePattern:  regexp.MustCompile(`workflow\.ExecuteActivity\s*\(\s*ctx\s*,\s*(\w+)`),
		TargetTemplate: `ctx.ExecuteActivity($1`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-sleep",
		SourcePattern:  regexp.MustCompile(`workflow\.Sleep\s*\(\s*ctx\s*,`),
		TargetTemplate: `ctx.Sleep(`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-get-signal",
		SourcePattern:  regexp.MustCompile(`workflow\.GetSignalChannel\s*\(\s*ctx\s*,\s*['"]?(\w+)`),
		TargetTemplate: `ctx.GetSignalChannel("$1")`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-new-client",
		SourcePattern:  regexp.MustCompile(`client\.Dial\s*\(`),
		TargetTemplate: `velocity.NewClient(`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-new-worker",
		SourcePattern:  regexp.MustCompile(`worker\.New\s*\(\s*c\s*,`),
		TargetTemplate: `velocity.NewWorker(`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-register-workflow",
		SourcePattern:  regexp.MustCompile(`w\.RegisterWorkflow\s*\(`),
		TargetTemplate: `w.RegisterWorkflow(`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-register-activity",
		SourcePattern:  regexp.MustCompile(`w\.RegisterActivity\s*\(`),
		TargetTemplate: `w.RegisterActivity(`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-start-workflow",
		SourcePattern:  regexp.MustCompile(`c\.ExecuteWorkflow\s*\(\s*ctx\s*,`),
		TargetTemplate: `client.ExecuteWorkflow(ctx,`,
		SourceFramework: "temporal",
	},
}

// ─── Restate → Velocity Patterns ─────────────────────────────────────────────

var RestatePatterns = []*MigrationPattern{
	{
		Name:           "restate-import",
		SourcePattern:  regexp.MustCompile(`"github\.com/restatedev/sdk-go"`),
		TargetTemplate: `"github.com/velocity-workflow/velocity-sdk-go"`,
		SourceFramework: "restate",
	},
	{
		Name:           "restate-service-func",
		SourcePattern:  regexp.MustCompile(`func\s+(\w+)\s*\(ctx\s+\*restate\.Context`),
		TargetTemplate: `func $1(ctx *velocity.WorkflowContext`,
		SourceFramework: "restate",
	},
	{
		Name:           "restate-ctx-call",
		SourcePattern:  regexp.MustCompile(`ctx\.Call\s*\(\s*(\w+)\s*,`),
		TargetTemplate: `ctx.ExecuteActivity($1,`,
		SourceFramework: "restate",
	},
	{
		Name:           "restate-ctx-sleep",
		SourcePattern:  regexp.MustCompile(`ctx\.Sleep\s*\(`),
		TargetTemplate: `ctx.Sleep(`,
		SourceFramework: "restate",
	},
	{
		Name:           "restate-ctx-get",
		SourcePattern:  regexp.MustCompile(`ctx\.Get\s*\(\s*['"](\w+)['"]`),
		TargetTemplate: `ctx.GetState("$1")`,
		SourceFramework: "restate",
	},
	{
		Name:           "restate-ctx-set",
		SourcePattern:  regexp.MustCompile(`ctx\.Set\s*\(\s*['"](\w+)['"]`),
		TargetTemplate: `ctx.SetState("$1"`,
		SourceFramework: "restate",
	},
}

// ─── DBOS → Velocity Patterns ────────────────────────────────────────────────

var DBOSPatterns = []*MigrationPattern{
	{
		Name:           "dbos-import",
		SourcePattern:  regexp.MustCompile(`"github\.com/dbos-inc/dbos-go"`),
		TargetTemplate: `"github.com/velocity-workflow/velocity-sdk-go"`,
		SourceFramework: "dbos",
	},
	{
		Name:           "dbos-workflow-func",
		SourcePattern:  regexp.MustCompile(`func\s+(\w+)\s*\(ctx\s+\*dbos\.WorkflowContext`),
		TargetTemplate: `func $1(ctx *velocity.WorkflowContext`,
		SourceFramework: "dbos",
	},
	{
		Name:           "dbos-transaction-func",
		SourcePattern:  regexp.MustCompile(`func\s+(\w+)\s*\(ctx\s+\*dbos\.TransactionContext`),
		TargetTemplate: `func $1(ctx *velocity.ActivityContext`,
		SourceFramework: "dbos",
	},
	{
		Name:           "dbos-execute-activity",
		SourcePattern:  regexp.MustCompile(`ctx\.Run\s*\(`),
		TargetTemplate: `ctx.ExecuteActivity(`,
		SourceFramework: "dbos",
	},
	{
		Name:           "dbos-sleep",
		SourcePattern:  regexp.MustCompile(`dbos\.Sleep\s*\(\s*ctx\s*,`),
		TargetTemplate: `ctx.Sleep(`,
		SourceFramework: "dbos",
	},
	{
		Name:           "dbos-recv",
		SourcePattern:  regexp.MustCompile(`dbos\.Recv\s*\(\s*ctx\s*,`),
		TargetTemplate: `ctx.Recv(`,
		SourceFramework: "dbos",
	},
}

// AllPatterns combines all framework patterns.
var AllPatterns = append(append(TemporalPatterns, RestatePatterns...), DBOSPatterns...)

// ─── Framework Detection ─────────────────────────────────────────────────────

// DetectResult holds the detected framework and confidence.
type DetectResult struct {
	Framework  string
	Confidence float64
	Evidence   []string
}

// DetectFramework detects which framework a Go file uses.
func DetectFramework(content string) DetectResult {
	scores := map[string]float64{"temporal": 0, "restate": 0, "dbos": 0}
	evidence := map[string][]string{}

	checks := []struct {
		pattern   *regexp.Regexp
		framework string
		score     float64
		desc      string
	}{
		{regexp.MustCompile(`go\.temporal\.io/sdk`), "temporal", 3, "Temporal SDK import"},
		{regexp.MustCompile(`workflow\.Context`), "temporal", 2, "workflow.Context usage"},
		{regexp.MustCompile(`workflow\.ExecuteActivity`), "temporal", 2, "workflow.ExecuteActivity"},
		{regexp.MustCompile(`restatedev/sdk-go`), "restate", 3, "Restate SDK import"},
		{regexp.MustCompile(`restate\.Context`), "restate", 2, "restate.Context usage"},
		{regexp.MustCompile(`ctx\.Call\(`), "restate", 1, "ctx.Call()"},
		{regexp.MustCompile(`dbos-inc/dbos-go`), "dbos", 3, "DBOS SDK import"},
		{regexp.MustCompile(`dbos\.WorkflowContext`), "dbos", 2, "dbos.WorkflowContext"},
		{regexp.MustCompile(`dbos\.TransactionContext`), "dbos", 2, "dbos.TransactionContext"},
	}

	for _, c := range checks {
		if c.pattern.MatchString(content) {
			scores[c.framework] += c.score
			evidence[c.framework] = append(evidence[c.framework], c.desc)
		}
	}

	best := "temporal"
	bestScore := 0.0
	for fw, score := range scores {
		if score > bestScore {
			best = fw
			bestScore = score
		}
	}

	total := 0.0
	for _, s := range scores {
		total += s
	}
	confidence := 0.0
	if total > 0 {
		confidence = bestScore / total
	}

	return DetectResult{
		Framework:  best,
		Confidence: confidence,
		Evidence:   evidence[best],
	}
}

// ─── File Migration ──────────────────────────────────────────────────────────

// FileResult holds the result of migrating a single file.
type FileResult struct {
	SourcePath       string
	OutputPath       string
	Success          bool
	Error            string
	DetectedFramework string
	Transformations  int
}

// MigrateFile migrates a single Go file's content.
func MigrateFile(content string, sourceFramework string) (string, FileResult) {
	result := FileResult{Success: true}

	// Auto-detect if needed
	if sourceFramework == "auto" {
		detection := DetectFramework(content)
		result.DetectedFramework = detection.Framework
		if detection.Confidence < 0.3 {
			result.Success = false
			result.Error = fmt.Sprintf("low confidence: %s (%.2f)", detection.Framework, detection.Confidence)
			return content, result
		}
		sourceFramework = detection.Framework
	} else {
		result.DetectedFramework = sourceFramework
	}

	// Select patterns
	var patterns []*MigrationPattern
	switch sourceFramework {
	case "temporal":
		patterns = TemporalPatterns
	case "restate":
		patterns = RestatePatterns
	case "dbos":
		patterns = DBOSPatterns
	default:
		result.Success = false
		result.Error = fmt.Sprintf("unknown framework: %s", sourceFramework)
		return content, result
	}

	// Apply transformations
	migrated := content
	count := 0
	for _, p := range patterns {
		newText := p.SourcePattern.ReplaceAllString(migrated, p.TargetTemplate)
		if newText != migrated {
			count++
			migrated = newText
		}
	}
	result.Transformations = count

	return migrated, result
}

// ─── Project Scanner ─────────────────────────────────────────────────────────

var skipDirs = map[string]bool{
	"vendor": true, ".git": true, "node_modules": true,
	"bin": true, "obj": true, "target": true,
}

// ScanGoFiles recursively finds all .go files in a directory.
func ScanGoFiles(rootDir string) ([]string, error) {
	var files []string
	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // skip inaccessible
		}
		if info.IsDir() && skipDirs[info.Name()] {
			return filepath.SkipDir
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".go") {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

// HasWorkflowContent checks if a Go file contains workflow-related patterns.
func HasWorkflowContent(content string) bool {
	indicators := []string{
		"go.temporal.io", "restatedev", "dbos-inc",
		"workflow.Context", "activity.Context",
		"ExecuteActivity", "workflow.Sleep",
		"restate.Context", "dbos.WorkflowContext",
	}
	for _, ind := range indicators {
		if strings.Contains(content, ind) {
			return true
		}
	}
	return false
}

// ─── Bulk Migration ──────────────────────────────────────────────────────────

// BulkResult holds the result of a bulk migration.
type BulkResult struct {
	TotalFiles int
	Migrated   int
	Failed     int
	Skipped    int
	Results    []FileResult
}

// BulkMigrate migrates all Go workflow files in a directory.
func BulkMigrate(sourceDir, outputDir, sourceFramework string, dryRun bool) (*BulkResult, error) {
	result := &BulkResult{}

	files, err := ScanGoFiles(sourceDir)
	if err != nil {
		return nil, fmt.Errorf("scanning files: %w", err)
	}
	result.TotalFiles = len(files)

	for _, filePath := range files {
		content, err := os.ReadFile(filePath)
		if err != nil {
			result.Failed++
			result.Results = append(result.Results, FileResult{
				SourcePath: filePath,
				Success:    false,
				Error:      err.Error(),
			})
			continue
		}

		if !HasWorkflowContent(string(content)) {
			result.Skipped++
			continue
		}

		migrated, fileResult := MigrateFile(string(content), sourceFramework)
		fileResult.SourcePath = filePath

		if fileResult.Success && !dryRun {
			relPath, _ := filepath.Rel(sourceDir, filePath)
			outPath := filepath.Join(outputDir, relPath)
			os.MkdirAll(filepath.Dir(outPath), 0755)
			if err := os.WriteFile(outPath, []byte(migrated), 0644); err != nil {
				fileResult.Success = false
				fileResult.Error = err.Error()
				result.Failed++
			} else {
				fileResult.OutputPath = outPath
				result.Migrated++
			}
		} else if fileResult.Success {
			result.Migrated++
		} else {
			result.Failed++
		}

		result.Results = append(result.Results, fileResult)
	}

	return result, nil
}
