// Package main implements a production benchmark client for Temporal.
//
// Connects to a REAL Temporal server via gRPC and measures throughput,
// latency, and error rates across equivalent workloads.
//
// Usage:
//
//	go run main.go --temporal-url localhost:7233 --profile standard
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// ─── Workflow & Activity Definitions ────────────────────────────────────────

// SimpleWorkflow is the basic benchmark workflow.
func SimpleWorkflow(ctx workflow.Context, input string) (string, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var result string
	err := workflow.ExecuteActivity(ctx, SimpleActivity, input).Get(ctx, &result)
	return result, err
}

// SimpleActivity is the basic benchmark activity.
func SimpleActivity(ctx context.Context, input string) (string, error) {
	return fmt.Sprintf("ok:%s:%d", input, time.Now().UnixMilli()), nil
}

// SignalWorkflow waits for N signals then completes.
func SignalWorkflow(ctx workflow.Context, expectedSignals int) (int, error) {
	received := 0
	for received < expectedSignals {
		var signal string
		ch := workflow.GetSignalChannel(ctx, fmt.Sprintf("signal-%d", received))
		if err := ch.Receive(ctx, &signal); err != nil {
			return received, err
		}
		received++
	}
	return received, nil
}

// MultiStepWorkflow executes N sequential activities.
func MultiStepWorkflow(ctx workflow.Context, steps int) (int, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	completed := 0
	for i := 0; i < steps; i++ {
		var result string
		err := workflow.ExecuteActivity(ctx, SimpleActivity, fmt.Sprintf("step-%d", i)).Get(ctx, &result)
		if err != nil {
			return completed, err
		}
		completed++
	}
	return completed, nil
}

// ─── Result Types ────────────────────────────────────────────────────────────

type WorkloadResult struct {
	Name              string  `json:"name"`
	Description       string  `json:"description"`
	TotalOps          uint64  `json:"total_operations"`
	SuccessOps        uint64  `json:"successful_operations"`
	FailedOps         uint64  `json:"failed_operations"`
	OpsPerSecond      float64 `json:"ops_per_second"`
	LatencyP50Us      float64 `json:"latency_p50_us"`
	LatencyP99Us      float64 `json:"latency_p99_us"`
	LatencyP999Us     float64 `json:"latency_p999_us"`
	LatencyMeanUs     float64 `json:"latency_mean_us"`
	ErrorRatePct      float64 `json:"error_rate_pct"`
}

type BenchmarkReport struct {
	Engine    string           `json:"engine"`
	Version   string           `json:"engine_version"`
	Profile   string           `json:"profile"`
	Timestamp string           `json:"timestamp"`
	Workloads []WorkloadResult `json:"workloads"`
}

// ─── Workload Runner ────────────────────────────────────────────────────────

type workloadConfig struct {
	name        string
	description string
	count       uint64
	concurrency int
}

func runSimpleWorkflows(ctx context.Context, c client.Client, cfg workloadConfig) WorkloadResult {
	var latencies []float64
	var success, fail atomic.Uint64
	benchStart := time.Now()

	var wg sync.WaitGroup
	sem := make(chan struct{}, cfg.concurrency)

	for i := uint64(0); i < cfg.count; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx uint64) {
			defer wg.Done()
			defer func() { <-sem }()

			start := time.Now()
			wfID := fmt.Sprintf("simple-%d-%d", time.Now().UnixNano(), idx)
			run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
				ID:        wfID,
				TaskQueue: "bench-queue",
			}, SimpleWorkflow, fmt.Sprintf("bench-%d", idx))

			if err != nil {
				fail.Add(1)
				return
			}

			var result string
			err = run.Get(ctx, &result)
			elapsed := float64(time.Since(start).Microseconds())

			if err != nil {
				fail.Add(1)
			} else {
				success.Add(1)
				latencies = append(latencies, elapsed)
			}
		}(i)
	}
	wg.Wait()

	return computeResult(cfg, success.Load(), fail.Load(), latencies, time.Since(benchStart))
}

func runMultiStepWorkflows(ctx context.Context, c client.Client, cfg workloadConfig, steps int) WorkloadResult {
	var latencies []float64
	var success, fail atomic.Uint64
	benchStart := time.Now()

	var wg sync.WaitGroup
	sem := make(chan struct{}, cfg.concurrency)

	for i := uint64(0); i < cfg.count; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx uint64) {
			defer wg.Done()
			defer func() { <-sem }()

			start := time.Now()
			wfID := fmt.Sprintf("multistep-%d-%d", time.Now().UnixNano(), idx)
			run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
				ID:        wfID,
				TaskQueue: "bench-queue",
			}, MultiStepWorkflow, steps)

			if err != nil {
				fail.Add(1)
				return
			}

			var result int
			err = run.Get(ctx, &result)
			elapsed := float64(time.Since(start).Microseconds())

			if err != nil {
				fail.Add(1)
			} else {
				success.Add(1)
				latencies = append(latencies, elapsed)
			}
		}(i)
	}
	wg.Wait()

	return computeResult(cfg, success.Load(), fail.Load(), latencies, time.Since(benchStart))
}

func runSignalWorkflows(ctx context.Context, c client.Client, cfg workloadConfig, numSignals int) WorkloadResult {
	var latencies []float64
	var success, fail atomic.Uint64
	benchStart := time.Now()

	var wg sync.WaitGroup
	sem := make(chan struct{}, cfg.concurrency)

	for i := uint64(0); i < cfg.count; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx uint64) {
			defer wg.Done()
			defer func() { <-sem }()

			start := time.Now()
			wfID := fmt.Sprintf("signal-%d-%d", time.Now().UnixNano(), idx)
			run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
				ID:        wfID,
				TaskQueue: "bench-queue",
			}, SignalWorkflow, numSignals)

			if err != nil {
				fail.Add(1)
				return
			}

			// Send signals
			for s := 0; s < numSignals; s++ {
				sigErr := c.SignalWorkflow(ctx, wfID, "", fmt.Sprintf("signal-%d", s), "ping")
				if sigErr != nil {
					fail.Add(1)
					return
				}
			}

			var result int
			err = run.Get(ctx, &result)
			elapsed := float64(time.Since(start).Microseconds())

			if err != nil {
				fail.Add(1)
			} else {
				success.Add(1)
				latencies = append(latencies, elapsed)
			}
		}(i)
	}
	wg.Wait()

	return computeResult(cfg, success.Load(), fail.Load(), latencies, time.Since(benchStart))
}

func computeResult(cfg workloadConfig, success, fail uint64, latencies []float64, wall time.Duration) WorkloadResult {
	wallSec := wall.Seconds()
	opsPerSec := float64(success) / wallSec
	if wallSec <= 0 {
		opsPerSec = 0
	}

	sort.Float64s(latencies)
	n := len(latencies)
	p50, p99, p999, mean := 0.0, 0.0, 0.0, 0.0
	if n > 0 {
		p50 = latencies[int(float64(n)*0.50)]
		p99 = latencies[int(math.Min(float64(n)*0.99, float64(n-1)))]
		p999 = latencies[int(math.Min(float64(n)*0.999, float64(n-1)))]
		sum := 0.0
		for _, l := range latencies {
			sum += l
		}
		mean = sum / float64(n)
	}

	total := success + fail
	errRate := 0.0
	if total > 0 {
		errRate = float64(fail) / float64(total) * 100.0
	}

	return WorkloadResult{
		Name:          cfg.name,
		Description:   cfg.description,
		TotalOps:      cfg.count,
		SuccessOps:    success,
		FailedOps:     fail,
		OpsPerSecond:  math.Round(opsPerSec*10) / 10,
		LatencyP50Us:  math.Round(p50*10) / 10,
		LatencyP99Us:  math.Round(p99*10) / 10,
		LatencyP999Us: math.Round(p999*10) / 10,
		LatencyMeanUs: math.Round(mean*10) / 10,
		ErrorRatePct:  math.Round(errRate*100) / 100,
	}
}

// ─── Main ────────────────────────────────────────────────────────────────────

func main() {
	// Parse flags
	temporalURL := "localhost:7233"
	profile := "standard"
	outputFile := ""
	outputFormat := "markdown"

	for i := 1; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--temporal-url":
			i++
			if i < len(os.Args) {
				temporalURL = os.Args[i]
			}
		case "--profile":
			i++
			if i < len(os.Args) {
				profile = os.Args[i]
			}
		case "--output":
			i++
			if i < len(os.Args) {
				outputFile = os.Args[i]
			}
		case "--format":
			i++
			if i < len(os.Args) {
				outputFormat = os.Args[i]
			}
		}
	}

	fmt.Println("╔══════════════════════════════════════════════════════════╗")
	fmt.Println("║  Temporal Production Benchmark Client                    ║")
	fmt.Println("║  Real Temporal server. Real gRPC. Real persistence.      ║")
	fmt.Println("╚══════════════════════════════════════════════════════════╝")
	fmt.Printf("Target:  %s\n", temporalURL)
	fmt.Printf("Profile: %s\n\n", profile)

	// Connect to Temporal
	c, err := client.Dial(client.Options{
		HostPort:  temporalURL,
		Namespace: "default",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Failed to connect to Temporal at %s: %v\n", temporalURL, err)
		os.Exit(1)
	}
	defer c.Close()
	fmt.Println("Connected to Temporal server.")

	// Start worker
	w := worker.New(c, "bench-queue", worker.Options{})
	w.RegisterWorkflow(SimpleWorkflow)
	w.RegisterWorkflow(SignalWorkflow)
	w.RegisterWorkflow(MultiStepWorkflow)
	w.RegisterActivity(SimpleActivity)

	ctx := context.Background()
	if err := w.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Failed to start worker: %v\n", err)
		os.Exit(1)
	}
	defer w.Stop()
	fmt.Println("Worker started on task queue 'bench-queue'.")
	fmt.Println()

	// Profile multipliers
	mult := 1.0
	switch profile {
	case "quick":
		mult = 0.1
	case "stress":
		mult = 10.0
	}

	// Run workloads
	workloads := []struct {
		cfg     workloadConfig
		runFunc func() WorkloadResult
	}{
		{
			cfg: workloadConfig{"simple_workflow", "Start → activity → complete", uint64(500 * mult), 10},
			runFunc: func() WorkloadResult {
				return runSimpleWorkflows(ctx, c, workloadConfig{"simple_workflow", "Start → activity → complete", uint64(500 * mult), 10})
			},
		},
		{
			cfg: workloadConfig{"high_step", "Single workflow with 10 steps", uint64(200 * mult), 5},
			runFunc: func() WorkloadResult {
				return runMultiStepWorkflows(ctx, c, workloadConfig{"high_step", "Single workflow with 10 steps", uint64(200 * mult), 5}, 10)
			},
		},
		{
			cfg: workloadConfig{"signal_storm", "Start → send 10 signals → complete", uint64(100 * mult), 5},
			runFunc: func() WorkloadResult {
				return runSignalWorkflows(ctx, c, workloadConfig{"signal_storm", "Start → send 10 signals → complete", uint64(100 * mult), 5}, 10)
			},
		},
		{
			cfg: workloadConfig{"concurrent_100", "100 concurrent workflows", uint64(500 * mult), 100},
			runFunc: func() WorkloadResult {
				return runSimpleWorkflows(ctx, c, workloadConfig{"concurrent_100", "100 concurrent workflows", uint64(500 * mult), 100})
			},
		},
		{
			cfg: workloadConfig{"throughput_ceiling", "Maximum sustainable throughput", uint64(5000 * mult), 50},
			runFunc: func() WorkloadResult {
				return runSimpleWorkflows(ctx, c, workloadConfig{"throughput_ceiling", "Maximum sustainable throughput", uint64(5000 * mult), 50})
			},
		},
	}

	var results []WorkloadResult
	for _, wl := range workloads {
		fmt.Printf("  Running %s (%.0f ops)...\n", wl.cfg.name, wl.cfg.count)
		r := wl.runFunc()
		fmt.Printf("    -> %.1f ops/sec, p99=%.0fµs, errors=%.1f%%\n",
			r.OpsPerSecond, r.LatencyP99Us, r.ErrorRatePct)
		results = append(results, r)
	}

	// Build report
	report := BenchmarkReport{
		Engine:    "Temporal",
		Version:   "1.24+ (real server)",
		Profile:   profile,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Workloads: results,
	}

	// Output
	var output string
	switch outputFormat {
	case "json":
		data, _ := json.MarshalIndent(report, "", "  ")
		output = string(data)
	case "csv":
		output = formatCSV(report)
	default:
		output = formatMarkdown(report)
	}

	if outputFile != "" {
		os.WriteFile(outputFile, []byte(output), 0644)
		fmt.Printf("\nResults written to %s\n", outputFile)
	} else {
		fmt.Println()
		fmt.Println(output)
	}
}

func formatMarkdown(report BenchmarkReport) string {
	var b strings.Builder
	b.WriteString("# Temporal Production Benchmark\n\n")
	b.WriteString(fmt.Sprintf("**Engine:** %s %s  \n", report.Engine, report.Version))
	b.WriteString(fmt.Sprintf("**Profile:** %s  \n", report.Profile))
	b.WriteString(fmt.Sprintf("**Timestamp:** %s\n\n", report.Timestamp))

	b.WriteString("| Workload | ops/sec | p50 µs | p99 µs | p999 µs | Errors |\n")
	b.WriteString("|----------|---------|--------|--------|---------|--------|\n")
	for _, w := range report.Workloads {
		b.WriteString(fmt.Sprintf("| %s | %.1f | %.0f | %.0f | %.0f | %d/%d |\n",
			w.Name, w.OpsPerSecond, w.LatencyP50Us, w.LatencyP99Us, w.LatencyP999Us,
			w.FailedOps, w.TotalOps))
	}
	return b.String()
}

func formatCSV(report BenchmarkReport) string {
	var b strings.Builder
	b.WriteString("workload,ops_per_sec,p50_us,p99_us,p999_us,mean_us,success,fail,error_rate_pct\n")
	for _, w := range report.Workloads {
		b.WriteString(fmt.Sprintf("%s,%.1f,%.0f,%.0f,%.0f,%.0f,%d,%d,%.2f\n",
			w.Name, w.OpsPerSecond, w.LatencyP50Us, w.LatencyP99Us, w.LatencyP999Us,
			w.LatencyMeanUs, w.SuccessOps, w.FailedOps, w.ErrorRatePct))
	}
	return b.String()
}
