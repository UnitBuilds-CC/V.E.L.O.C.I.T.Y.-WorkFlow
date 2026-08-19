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
	// Search attributes
	{
		Name:           "temporal-search-attributes",
		SourcePattern:  regexp.MustCompile(`workflow\.GetSearchAttributes\s*\(`),
		TargetTemplate: `ctx.GetSearchAttributes(`,
		SourceFramework: "temporal",
	},
	// Memo
	{
		Name:           "temporal-memo",
		SourcePattern:  regexp.MustCompile(`workflow\.GetMemo\s*\(`),
		TargetTemplate: `ctx.GetMemo(`,
		SourceFramework: "temporal",
	},
	// Update handler
	{
		Name:           "temporal-update-handler",
		SourcePattern:  regexp.MustCompile(`workflow\.SetUpdateHandler\s*\(`),
		TargetTemplate: `ctx.SetUpdateHandler(`,
		SourceFramework: "temporal",
	},
	// Continue-as-new
	{
		Name:           "temporal-continue-as-new",
		SourcePattern:  regexp.MustCompile(`workflow\.ContinueAsNew\s*\(`),
		TargetTemplate: `ctx.ContinueAsNew(`,
		SourceFramework: "temporal",
	},
	// ─── Child Workflow Patterns ─────────────────────────────────────────────
	{
		Name:           "temporal-execute-child-workflow",
		SourcePattern:  regexp.MustCompile(`workflow\.ExecuteChildWorkflow\s*\(\s*ctx\s*,`),
		TargetTemplate: `ctx.ExecuteChildWorkflow(`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-with-child-options",
		SourcePattern:  regexp.MustCompile(`workflow\.WithChildOptions\s*\(\s*ctx\s*,`),
		TargetTemplate: `ctx.WithChildOptions(`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-child-workflow-future",
		SourcePattern:  regexp.MustCompile(`workflow\.ChildWorkflowFuture`),
		TargetTemplate: `velocity.ChildWorkflowFuture`,
		SourceFramework: "temporal",
	},
	// ─── Activity Options Patterns ───────────────────────────────────────────
	{
		Name:           "temporal-with-activity-options",
		SourcePattern:  regexp.MustCompile(`workflow\.WithActivityOptions\s*\(\s*ctx\s*,`),
		TargetTemplate: `ctx.WithActivityOptions(`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-activity-options",
		SourcePattern:  regexp.MustCompile(`workflow\.ActivityOptions\{`),
		TargetTemplate: `velocity.ActivityOptions{`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-execute-local-activity",
		SourcePattern:  regexp.MustCompile(`workflow\.ExecuteLocalActivity\s*\(\s*ctx\s*,`),
		TargetTemplate: `ctx.ExecuteLocalActivity(`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-with-local-activity-options",
		SourcePattern:  regexp.MustCompile(`workflow\.WithLocalActivityOptions\s*\(\s*ctx\s*,`),
		TargetTemplate: `ctx.WithLocalActivityOptions(`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-local-activity-options",
		SourcePattern:  regexp.MustCompile(`workflow\.LocalActivityOptions\{`),
		TargetTemplate: `velocity.LocalActivityOptions{`,
		SourceFramework: "temporal",
	},
	// ─── Coroutine & Concurrency Patterns ────────────────────────────────────
	{
		Name:           "temporal-workflow-go",
		SourcePattern:  regexp.MustCompile(`workflow\.Go\s*\(\s*ctx\s*,`),
		TargetTemplate: `ctx.Go(`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-workflow-await",
		SourcePattern:  regexp.MustCompile(`workflow\.Await\s*\(\s*ctx\s*,`),
		TargetTemplate: `ctx.Await(`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-workflow-await-with-timeout",
		SourcePattern:  regexp.MustCompile(`workflow\.AwaitWithTimeout\s*\(\s*ctx\s*,`),
		TargetTemplate: `ctx.AwaitWithTimeout(`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-new-future",
		SourcePattern:  regexp.MustCompile(`workflow\.NewFuture\s*\(\s*ctx\s*\)`),
		TargetTemplate: `ctx.NewFuture()`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-new-channel",
		SourcePattern:  regexp.MustCompile(`workflow\.NewChannel\s*\(\s*ctx\s*\)`),
		TargetTemplate: `ctx.NewChannel()`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-workflow-future",
		SourcePattern:  regexp.MustCompile(`workflow\.Future`),
		TargetTemplate: `velocity.Future`,
		SourceFramework: "temporal",
	},
	// ─── Relay/Nexus Operation Patterns ──────────────────────────────────────
	{
		Name:           "temporal-new-nexus-client",
		SourcePattern:  regexp.MustCompile(`workflow\.NewNexusClient\s*\(`),
		TargetTemplate: `ctx.NewRelayClient(`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-nexus-operation-future",
		SourcePattern:  regexp.MustCompile(`workflow\.NexusOperationFuture`),
		TargetTemplate: `velocity.RelayOperationFuture`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-nexus-execute-operation",
		SourcePattern:  regexp.MustCompile(`client\.ExecuteOperation\s*\(`),
		TargetTemplate: `relayClient.Execute(`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-nexus-operation-options",
		SourcePattern:  regexp.MustCompile(`workflow\.NexusOperationOptions\{`),
		TargetTemplate: `velocity.RelayOperationOptions{`,
		SourceFramework: "temporal",
	},
	// ─── Activity Context Patterns ───────────────────────────────────────────
	{
		Name:           "temporal-activity-get-info",
		SourcePattern:  regexp.MustCompile(`activity\.GetInfo\s*\(\s*ctx\s*\)`),
		TargetTemplate: `ctx.GetInfo()`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-activity-record-heartbeat",
		SourcePattern:  regexp.MustCompile(`activity\.RecordHeartbeat\s*\(\s*ctx\s*`),
		TargetTemplate: `ctx.RecordHeartbeat(`,
		SourceFramework: "temporal",
	},
	// ─── Workflow Context Patterns ───────────────────────────────────────────
	{
		Name:           "temporal-workflow-get-info",
		SourcePattern:  regexp.MustCompile(`workflow\.GetInfo\s*\(\s*ctx\s*\)`),
		TargetTemplate: `ctx.GetWorkflowInfo()`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-workflow-get-logger",
		SourcePattern:  regexp.MustCompile(`workflow\.GetLogger\s*\(\s*ctx\s*\)`),
		TargetTemplate: `ctx.Logger()`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-workflow-with-cancel",
		SourcePattern:  regexp.MustCompile(`workflow\.WithCancel\s*\(\s*ctx\s*\)`),
		TargetTemplate: `ctx.WithCancel()`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-signal-external-workflow",
		SourcePattern:  regexp.MustCompile(`workflow\.SignalExternalWorkflow\s*\(\s*ctx\s*,`),
		TargetTemplate: `ctx.SignalExternalWorkflow(`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-workflow-get-version",
		SourcePattern:  regexp.MustCompile(`workflow\.GetVersion\s*\(\s*ctx\s*,`),
		TargetTemplate: `ctx.GetVersion(`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-upsert-search-attributes",
		SourcePattern:  regexp.MustCompile(`workflow\.UpsertSearchAttributes\s*\(\s*ctx\s*,`),
		TargetTemplate: `ctx.UpsertSearchAttributes(`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-upsert-memo",
		SourcePattern:  regexp.MustCompile(`workflow\.UpsertMemo\s*\(\s*ctx\s*,`),
		TargetTemplate: `ctx.UpsertMemo(`,
		SourceFramework: "temporal",
	},
	// ─── Error Handling Patterns ─────────────────────────────────────────────
	{
		Name:           "temporal-new-application-error",
		SourcePattern:  regexp.MustCompile(`temporal\.NewApplicationError\s*\(`),
		TargetTemplate: `velocity.NewApplicationError(`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-canceled-error",
		SourcePattern:  regexp.MustCompile(`temporal\.CanceledError`),
		TargetTemplate: `velocity.CanceledError`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-import-temporal-package",
		SourcePattern:  regexp.MustCompile(`"go\.temporal\.io/sdk/temporal"`),
		TargetTemplate: `"github.com/velocity-workflow/velocity-sdk-go"`,
		SourceFramework: "temporal",
	},
	{
		Name:           "temporal-import-nexus-package",
		SourcePattern:  regexp.MustCompile(`"go\.temporal\.io/sdk/temporalnexus"`),
		TargetTemplate: `"github.com/velocity-workflow/velocity-sdk-go/relay"`,
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
	// Idempotency key
	{
		Name:           "restate-idempotency-key",
		SourcePattern:  regexp.MustCompile(`ctx\.IdempotencyKey\s*\(`),
		TargetTemplate: `ctx.IdempotencyKey(`,
		SourceFramework: "restate",
	},
	// Service client
	{
		Name:           "restate-service-client",
		SourcePattern:  regexp.MustCompile(`restate\.NewServiceClient\s*\(`),
		TargetTemplate: `velocity.NewClient(`,
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
	// Queue operations
	{
		Name:           "dbos-queue-enqueue",
		SourcePattern:  regexp.MustCompile(`dbos\.Enqueue\s*\(\s*ctx\s*,`),
		TargetTemplate: `ctx.Enqueue(`,
		SourceFramework: "dbos",
	},
	{
		Name:           "dbos-queue-dequeue",
		SourcePattern:  regexp.MustCompile(`dbos\.Dequeue\s*\(\s*ctx\s*,`),
		TargetTemplate: `ctx.Dequeue(`,
		SourceFramework: "dbos",
	},
	// HTTP handler
	{
		Name:           "dbos-http-handler",
		SourcePattern:  regexp.MustCompile(`dbos\.HTTPHandler\s*\(`),
		TargetTemplate: `velocity.HTTPHandler(`,
		SourceFramework: "dbos",
	},
}

// AllPatterns combines all framework patterns.
var AllPatterns = append(append(TemporalPatterns, RestatePatterns...), DBOSPatterns...)

// ─── Inter-Flavor Migration Patterns (Server ↔ Binary ↔ Embedded) ────────────

// InterFlavorPatternSets maps "source→target" to pattern lists for Velocity flavor-to-flavor migration.
var InterFlavorPatternSets = map[string][]*MigrationPattern{
	// ── Server → Binary ──────────────────────────────────────────────────────
	"server→binary": {
		{Name: "server-to-binary-import", SourcePattern: regexp.MustCompile(`"github\.com/velocity-workflow/velocity-sdk-go"`), TargetTemplate: `"github.com/velocity-workflow/velocity-sdk-go/binary"`, SourceFramework: "server"},
		{Name: "server-to-binary-execute-activity", SourcePattern: regexp.MustCompile(`ctx\.ExecuteActivity\(`), TargetTemplate: `ctx.Call(`, SourceFramework: "server"},
		{Name: "server-to-binary-child-workflow", SourcePattern: regexp.MustCompile(`ctx\.ExecuteChildWorkflow\(`), TargetTemplate: `ctx.Call(`, SourceFramework: "server"},
		{Name: "server-to-binary-get-signal", SourcePattern: regexp.MustCompile(`ctx\.GetSignalChannel\(`), TargetTemplate: `ctx.Promise(`, SourceFramework: "server"},
		{Name: "server-to-binary-wait-signal", SourcePattern: regexp.MustCompile(`ctx\.WaitForSignal\(`), TargetTemplate: `ctx.Await(`, SourceFramework: "server"},
		{Name: "server-to-binary-set-state", SourcePattern: regexp.MustCompile(`ctx\.SetState\(`), TargetTemplate: `ctx.Set(`, SourceFramework: "server"},
		{Name: "server-to-binary-get-state", SourcePattern: regexp.MustCompile(`ctx\.GetState\(`), TargetTemplate: `ctx.Get(`, SourceFramework: "server"},
		{Name: "server-to-binary-future", SourcePattern: regexp.MustCompile(`velocity\.Future`), TargetTemplate: `binary.Future`, SourceFramework: "server"},
		{Name: "server-to-binary-new-future", SourcePattern: regexp.MustCompile(`ctx\.NewFuture\(\)`), TargetTemplate: `ctx.NewPromise()`, SourceFramework: "server"},
		{Name: "server-to-binary-new-channel", SourcePattern: regexp.MustCompile(`ctx\.NewChannel\(\)`), TargetTemplate: `ctx.NewPromise()`, SourceFramework: "server"},
		{Name: "server-to-binary-relay-client", SourcePattern: regexp.MustCompile(`ctx\.NewRelayClient\(`), TargetTemplate: `ctx.NewServiceClient(`, SourceFramework: "server"},
		{Name: "server-to-binary-signal-external", SourcePattern: regexp.MustCompile(`ctx\.SignalExternalWorkflow\(`), TargetTemplate: `ctx.Send(`, SourceFramework: "server"},
	},
	// ── Server → Embedded ────────────────────────────────────────────────────
	"server→embedded": {
		{Name: "server-to-embedded-import", SourcePattern: regexp.MustCompile(`"github\.com/velocity-workflow/velocity-sdk-go"`), TargetTemplate: `"github.com/velocity-workflow/velocity-sdk-go/embedded"`, SourceFramework: "server"},
		{Name: "server-to-embedded-execute-activity", SourcePattern: regexp.MustCompile(`ctx\.ExecuteActivity\(`), TargetTemplate: `ctx.Invoke(`, SourceFramework: "server"},
		{Name: "server-to-embedded-child-workflow", SourcePattern: regexp.MustCompile(`ctx\.ExecuteChildWorkflow\(`), TargetTemplate: `ctx.StartChildWorkflow(`, SourceFramework: "server"},
		{Name: "server-to-embedded-get-signal", SourcePattern: regexp.MustCompile(`ctx\.GetSignalChannel\(`), TargetTemplate: `ctx.AwaitSignal(`, SourceFramework: "server"},
		{Name: "server-to-embedded-wait-signal", SourcePattern: regexp.MustCompile(`ctx\.WaitForSignal\(`), TargetTemplate: `ctx.Await(`, SourceFramework: "server"},
		{Name: "server-to-embedded-set-state", SourcePattern: regexp.MustCompile(`ctx\.SetState\(`), TargetTemplate: `ctx.SetState(`, SourceFramework: "server"},
		{Name: "server-to-embedded-get-state", SourcePattern: regexp.MustCompile(`ctx\.GetState\(`), TargetTemplate: `ctx.GetState(`, SourceFramework: "server"},
		{Name: "server-to-embedded-future", SourcePattern: regexp.MustCompile(`velocity\.Future`), TargetTemplate: `embedded.Future`, SourceFramework: "server"},
		{Name: "server-to-embedded-relay-client", SourcePattern: regexp.MustCompile(`ctx\.NewRelayClient\(`), TargetTemplate: `ctx.NewClient(`, SourceFramework: "server"},
	},
	// ── Binary → Server ──────────────────────────────────────────────────────
	"binary→server": {
		{Name: "binary-to-server-import", SourcePattern: regexp.MustCompile(`"github\.com/velocity-workflow/velocity-sdk-go/binary"`), TargetTemplate: `"github.com/velocity-workflow/velocity-sdk-go"`, SourceFramework: "binary"},
		{Name: "binary-to-server-call", SourcePattern: regexp.MustCompile(`ctx\.Call\(`), TargetTemplate: `ctx.ExecuteActivity(`, SourceFramework: "binary"},
		{Name: "binary-to-server-promise", SourcePattern: regexp.MustCompile(`ctx\.Promise\(`), TargetTemplate: `ctx.GetSignalChannel(`, SourceFramework: "binary"},
		{Name: "binary-to-server-await", SourcePattern: regexp.MustCompile(`ctx\.Await\(`), TargetTemplate: `ctx.WaitForSignal(`, SourceFramework: "binary"},
		{Name: "binary-to-server-set", SourcePattern: regexp.MustCompile(`ctx\.Set\(`), TargetTemplate: `ctx.SetState(`, SourceFramework: "binary"},
		{Name: "binary-to-server-get", SourcePattern: regexp.MustCompile(`ctx\.Get\(`), TargetTemplate: `ctx.GetState(`, SourceFramework: "binary"},
		{Name: "binary-to-server-future", SourcePattern: regexp.MustCompile(`binary\.Future`), TargetTemplate: `velocity.Future`, SourceFramework: "binary"},
		{Name: "binary-to-server-new-promise", SourcePattern: regexp.MustCompile(`ctx\.NewPromise\(\)`), TargetTemplate: `ctx.NewFuture()`, SourceFramework: "binary"},
		{Name: "binary-to-server-service-client", SourcePattern: regexp.MustCompile(`ctx\.NewServiceClient\(`), TargetTemplate: `ctx.NewRelayClient(`, SourceFramework: "binary"},
		{Name: "binary-to-server-send", SourcePattern: regexp.MustCompile(`ctx\.Send\(`), TargetTemplate: `ctx.SignalExternalWorkflow(`, SourceFramework: "binary"},
	},
	// ── Binary → Embedded ────────────────────────────────────────────────────
	"binary→embedded": {
		{Name: "binary-to-embedded-import", SourcePattern: regexp.MustCompile(`"github\.com/velocity-workflow/velocity-sdk-go/binary"`), TargetTemplate: `"github.com/velocity-workflow/velocity-sdk-go/embedded"`, SourceFramework: "binary"},
		{Name: "binary-to-embedded-call", SourcePattern: regexp.MustCompile(`ctx\.Call\(`), TargetTemplate: `ctx.Invoke(`, SourceFramework: "binary"},
		{Name: "binary-to-embedded-promise", SourcePattern: regexp.MustCompile(`ctx\.Promise\(`), TargetTemplate: `ctx.AwaitSignal(`, SourceFramework: "binary"},
		{Name: "binary-to-embedded-set", SourcePattern: regexp.MustCompile(`ctx\.Set\(`), TargetTemplate: `ctx.SetState(`, SourceFramework: "binary"},
		{Name: "binary-to-embedded-get", SourcePattern: regexp.MustCompile(`ctx\.Get\(`), TargetTemplate: `ctx.GetState(`, SourceFramework: "binary"},
		{Name: "binary-to-embedded-future", SourcePattern: regexp.MustCompile(`binary\.Future`), TargetTemplate: `embedded.Future`, SourceFramework: "binary"},
		{Name: "binary-to-embedded-service-client", SourcePattern: regexp.MustCompile(`ctx\.NewServiceClient\(`), TargetTemplate: `ctx.NewClient(`, SourceFramework: "binary"},
		{Name: "binary-to-embedded-send", SourcePattern: regexp.MustCompile(`ctx\.Send\(`), TargetTemplate: `ctx.Signal(`, SourceFramework: "binary"},
	},
	// ── Embedded → Server ────────────────────────────────────────────────────
	"embedded→server": {
		{Name: "embedded-to-server-import", SourcePattern: regexp.MustCompile(`"github\.com/velocity-workflow/velocity-sdk-go/embedded"`), TargetTemplate: `"github.com/velocity-workflow/velocity-sdk-go"`, SourceFramework: "embedded"},
		{Name: "embedded-to-server-invoke", SourcePattern: regexp.MustCompile(`ctx\.Invoke\(`), TargetTemplate: `ctx.ExecuteActivity(`, SourceFramework: "embedded"},
		{Name: "embedded-to-server-child-wf", SourcePattern: regexp.MustCompile(`ctx\.StartChildWorkflow\(`), TargetTemplate: `ctx.ExecuteChildWorkflow(`, SourceFramework: "embedded"},
		{Name: "embedded-to-server-await-signal", SourcePattern: regexp.MustCompile(`ctx\.AwaitSignal\(`), TargetTemplate: `ctx.GetSignalChannel(`, SourceFramework: "embedded"},
		{Name: "embedded-to-server-future", SourcePattern: regexp.MustCompile(`embedded\.Future`), TargetTemplate: `velocity.Future`, SourceFramework: "embedded"},
		{Name: "embedded-to-server-client", SourcePattern: regexp.MustCompile(`ctx\.NewClient\(`), TargetTemplate: `ctx.NewRelayClient(`, SourceFramework: "embedded"},
	},
	// ── Embedded → Binary ────────────────────────────────────────────────────
	"embedded→binary": {
		{Name: "embedded-to-binary-import", SourcePattern: regexp.MustCompile(`"github\.com/velocity-workflow/velocity-sdk-go/embedded"`), TargetTemplate: `"github.com/velocity-workflow/velocity-sdk-go/binary"`, SourceFramework: "embedded"},
		{Name: "embedded-to-binary-invoke", SourcePattern: regexp.MustCompile(`ctx\.Invoke\(`), TargetTemplate: `ctx.Call(`, SourceFramework: "embedded"},
		{Name: "embedded-to-binary-child-wf", SourcePattern: regexp.MustCompile(`ctx\.StartChildWorkflow\(`), TargetTemplate: `ctx.Call(`, SourceFramework: "embedded"},
		{Name: "embedded-to-binary-await-signal", SourcePattern: regexp.MustCompile(`ctx\.AwaitSignal\(`), TargetTemplate: `ctx.Promise(`, SourceFramework: "embedded"},
		{Name: "embedded-to-binary-set-state", SourcePattern: regexp.MustCompile(`ctx\.SetState\(`), TargetTemplate: `ctx.Set(`, SourceFramework: "embedded"},
		{Name: "embedded-to-binary-get-state", SourcePattern: regexp.MustCompile(`ctx\.GetState\(`), TargetTemplate: `ctx.Get(`, SourceFramework: "embedded"},
		{Name: "embedded-to-binary-future", SourcePattern: regexp.MustCompile(`embedded\.Future`), TargetTemplate: `binary.Future`, SourceFramework: "embedded"},
		{Name: "embedded-to-binary-client", SourcePattern: regexp.MustCompile(`ctx\.NewClient\(`), TargetTemplate: `ctx.NewServiceClient(`, SourceFramework: "embedded"},
		{Name: "embedded-to-binary-signal", SourcePattern: regexp.MustCompile(`ctx\.Signal\(`), TargetTemplate: `ctx.Send(`, SourceFramework: "embedded"},
	},
}

// GetInterFlavorPatterns returns patterns for a specific source→target migration.
func GetInterFlavorPatterns(source, target string) []*MigrationPattern {
	key := source + "→" + target
	return InterFlavorPatternSets[key]
}

// ─── Framework Detection ─────────────────────────────────────────────────────

// DetectResult holds the detected framework and confidence.
type DetectResult struct {
	Framework  string
	Confidence float64
	Evidence   []string
}

// DetectFramework detects which framework a Go file uses.
func DetectFramework(content string) DetectResult {
	scores := map[string]float64{"temporal": 0, "restate": 0, "dbos": 0, "server": 0, "binary": 0, "embedded": 0}
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
		{regexp.MustCompile(`workflow\.GetSearchAttributes`), "temporal", 1, "workflow.GetSearchAttributes"},
		{regexp.MustCompile(`workflow\.SetUpdateHandler`), "temporal", 1, "workflow.SetUpdateHandler"},
		{regexp.MustCompile(`workflow\.ContinueAsNew`), "temporal", 1, "workflow.ContinueAsNew"},
		{regexp.MustCompile(`restatedev/sdk-go`), "restate", 3, "Restate SDK import"},
		{regexp.MustCompile(`restate\.Context`), "restate", 2, "restate.Context usage"},
		{regexp.MustCompile(`ctx\.Call\(`), "restate", 1, "ctx.Call()"},
		{regexp.MustCompile(`restate\.NewServiceClient`), "restate", 1, "restate.NewServiceClient"},
		{regexp.MustCompile(`dbos-inc/dbos-go`), "dbos", 3, "DBOS SDK import"},
		{regexp.MustCompile(`dbos\.WorkflowContext`), "dbos", 2, "dbos.WorkflowContext"},
		{regexp.MustCompile(`dbos\.TransactionContext`), "dbos", 2, "dbos.TransactionContext"},
		{regexp.MustCompile(`dbos\.Enqueue`), "dbos", 1, "dbos.Enqueue"},
		{regexp.MustCompile(`dbos\.HTTPHandler`), "dbos", 1, "dbos.HTTPHandler"},
		// Velocity Server patterns
		{regexp.MustCompile(`velocity-workflow/velocity-sdk-go"`), "server", 3, "Velocity Server SDK import"},
		{regexp.MustCompile(`ctx\.ExecuteActivity\(`), "server", 1, "ctx.ExecuteActivity()"},
		{regexp.MustCompile(`ctx\.GetSignalChannel\(`), "server", 1, "ctx.GetSignalChannel()"},
		{regexp.MustCompile(`ctx\.WaitForSignal\(`), "server", 1, "ctx.WaitForSignal()"},
		// Velocity Binary patterns
		{regexp.MustCompile(`velocity-sdk-go/binary`), "binary", 3, "Velocity Binary SDK import"},
		{regexp.MustCompile(`ctx\.NewServiceClient\(`), "binary", 1, "ctx.NewServiceClient()"},
		{regexp.MustCompile(`ctx\.Send\(`), "binary", 1, "ctx.Send()"},
		// Velocity Embedded patterns
		{regexp.MustCompile(`velocity-sdk-go/embedded`), "embedded", 3, "Velocity Embedded SDK import"},
		{regexp.MustCompile(`ctx\.Invoke\(`), "embedded", 1, "ctx.Invoke()"},
		{regexp.MustCompile(`ctx\.AwaitSignal\(`), "embedded", 1, "ctx.AwaitSignal()"},
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
// targetFlavor specifies the target Velocity flavor: "server", "binary", or "embedded".
// Pass "" or "auto" to use the default target (server for temporal/restate, embedded for dbos).
func MigrateFile(content string, sourceFramework string, targetFlavor ...string) (string, FileResult) {
	result := FileResult{Success: true}
	target := "server" // default
	if len(targetFlavor) > 0 && targetFlavor[0] != "" {
		target = targetFlavor[0]
	}

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

	// Check if this is an inter-flavor migration (Velocity source → different Velocity target)
	velocityFlavors := map[string]bool{"server": true, "binary": true, "embedded": true}
	if velocityFlavors[sourceFramework] && sourceFramework != target {
		patterns := GetInterFlavorPatterns(sourceFramework, target)
		if patterns == nil {
			result.Success = false
			result.Error = fmt.Sprintf("no inter-flavor patterns: %s → %s", sourceFramework, target)
			return content, result
		}
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

	// Select patterns for external framework migrations
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
		"velocity-workflow/velocity-sdk-go", "velocity-sdk-go/binary",
		"velocity-sdk-go/embedded", "ctx.Invoke(", "ctx.Call(",
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
// targetFlavor specifies the target Velocity flavor (server/binary/embedded).
func BulkMigrate(sourceDir, outputDir, sourceFramework string, dryRun bool, targetFlavor ...string) (*BulkResult, error) {
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

		migrated, fileResult := MigrateFile(string(content), sourceFramework, targetFlavor...)
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
