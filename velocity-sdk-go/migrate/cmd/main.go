// Command velocity-migrate-go provides CLI for migrating Go workflows
// from Temporal, Restate, or DBOS to Velocity.
//
// Usage:
//
//	go run velocity-sdk-go/migrate/cmd/main.go --src ./my_project --from temporal
//	go run velocity-sdk-go/migrate/cmd/main.go --detect ./my_project
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/velocity-workflow/velocity-sdk-go/migrate"
)

func main() {
	src := flag.String("src", "", "Source file or directory")
	from := flag.String("from", "auto", "Source framework (temporal, restate, dbos, auto)")
	output := flag.String("output", "", "Output file or directory")
	dryRun := flag.Bool("dry-run", false, "Detect and report without writing")
	detect := flag.Bool("detect", false, "Detect framework in directory")
	flag.Parse()

	if *src == "" {
		fmt.Fprintf(os.Stderr, "Error: --src is required\n")
		flag.Usage()
		os.Exit(1)
	}

	// Mode: detect
	if *detect {
		info, err := os.Stat(*src)
		if err != nil || !info.IsDir() {
			fmt.Fprintf(os.Stderr, "Error: --detect requires a directory: %s\n", *src)
			os.Exit(1)
		}

		files, _ := migrate.ScanGoFiles(*src)
		fmt.Printf("Scanning %d Go files in %s...\n", len(files), *src)

		for _, f := range files {
			content, err := os.ReadFile(f)
			if err != nil {
				continue
			}
			result := migrate.DetectFramework(string(content))
			if result.Confidence > 0.3 {
				relPath, _ := filepath.Rel(*src, f)
				fmt.Printf("  %s: %s (%.0f%%) [%s]\n",
					relPath, result.Framework, result.Confidence*100,
					strings.Join(result.Evidence, ", "))
			}
		}
		return
	}

	// Mode: single file
	info, err := os.Stat(*src)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s not found\n", *src)
		os.Exit(1)
	}

	if !info.IsDir() {
		content, err := os.ReadFile(*src)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading file: %v\n", err)
			os.Exit(1)
		}

		migrated, result := migrate.MigrateFile(string(content), *from)
		if !result.Success {
			fmt.Fprintf(os.Stderr, "Migration failed: %s\n", result.Error)
			os.Exit(1)
		}

		if *output != "" {
			os.WriteFile(*output, []byte(migrated), 0644)
			fmt.Printf("Written to: %s\n", *output)
		} else {
			fmt.Print(migrated)
		}
		fmt.Printf("\nDetected: %s\n", result.DetectedFramework)
		fmt.Printf("Transformations: %d\n", result.Transformations)
		return
	}

	// Mode: directory
	outputDir := *output
	if outputDir == "" {
		outputDir = filepath.Join(*src, "..", "velocity-migrated")
	}

	fmt.Printf("Scanning: %s\n", *src)
	fmt.Printf("Output: %s\n", func() string {
		if *dryRun {
			return "(dry run)"
		}
		return outputDir
	}())
	fmt.Printf("Source framework: %s\n\n", *from)

	result, err := migrate.BulkMigrate(*src, outputDir, *from, *dryRun)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Results:\n")
	fmt.Printf("  Total files: %d\n", result.TotalFiles)
	fmt.Printf("  Migrated: %d\n", result.Migrated)
	fmt.Printf("  Failed: %d\n", result.Failed)
	fmt.Printf("  Skipped: %d\n", result.Skipped)

	for _, r := range result.Results {
		status := "OK"
		if !r.Success {
			status = "FAIL"
		}
		relPath, _ := filepath.Rel(*src, r.SourcePath)
		fmt.Printf("  [%s] %s (%s, %d changes)\n",
			status, relPath, r.DetectedFramework, r.Transformations)
		if r.Error != "" {
			fmt.Printf("         Error: %s\n", r.Error)
		}
	}
}
