import {
  BarChart,
  Callout,
  ChartContainer,
  H1,
  MetricsGrid,
  ReportSection,
  ReportShell,
  Stack,
  Table,
  Text,
} from "qoder/canvas";

const headlineMetrics = [
  { label: "Rust Crates", value: "17", tone: "positive" as const },
  { label: "TypeScript Pkgs", value: "8", tone: "positive" as const },
  { label: "Python Pkgs", value: "2", tone: "positive" as const },
  { label: "Platforms", value: "5", tone: "positive" as const },
  { label: "SDK Channels", value: "4", tone: "positive" as const },
];

const versionBumps = [
  ["Rust (Cargo.toml)", "17", "0.1.0 → 1.0.0", "3 with publishing metadata, 5 with publish=false"],
  ["TypeScript (package.json)", "8", "0.1.0 → 1.0.0", "1 with npm publishConfig + repository"],
  ["Python (pyproject.toml)", "2", "0.1.0 → 1.0.0", "Production/Stable classifier, Apache-2.0"],
  ["Java (pom.xml)", "1", "0.1.0 → 1.0.0", "Maven Central: licenses, scm, nexus-staging"],
  ["Helm (Chart.yaml)", "1", "0.1.0 → 1.0.0", "appVersion already 1.0.0"],
];

const platformCategories = [
  "linux-amd64",
  "linux-arm64",
  "darwin-amd64",
  "darwin-arm64",
  "windows-amd64",
];
const binaryCounts = [3, 3, 3, 3, 3]; // 3 server variants per platform

const releasePipeline = [
  ["1. Tag v1.0.0", "git push origin v1.0.0", "Triggers release workflow"],
  ["2. Cross-compile", "5 targets × 3 servers", "15 native binaries"],
  ["3. Docker image", "GHCR (amd64 + arm64)", "Multi-arch manifest"],
  ["4. npm publish", "@velocity-workflow/sdk@1.0.0", "TypeScript SDK"],
  ["5. PyPI publish", "velocity-workflow==1.0.0", "Python SDK"],
  ["6. Maven deploy", "io.velocity:velocity-sdk-java:1.0.0", "Java SDK"],
  ["7. Go tag", "sdk-go/v1.0.0", "Go module proxy"],
  ["8. Deploy staging", "Helm upgrade", "Smoke tests"],
  ["9. Deploy production", "Helm upgrade (3-15 replicas)", "Smoke tests"],
  ["10. GitHub Release", "Binary attachments + notes", "User-facing release"],
];

const installMethods = [
  ["Docker", "docker pull ghcr.io/velocity-workflow/velocity-workflow-server:1.0.0", "Recommended for production"],
  ["Helm", "helm install velocity velocity/velocity --version 1.0.0", "Full K8s deployment"],
  ["npm", "npm install @velocity-workflow/sdk@1.0.0", "TypeScript SDK"],
  ["pip", "pip install velocity-workflow==1.0.0", "Python SDK"],
  ["Maven", "io.velocity:velocity-sdk-java:1.0.0", "Java SDK"],
  ["Go", "go get github.com/velocity-workflow/sdk-go@v1.0.0", "Go SDK"],
  ["cargo", "cargo install velocity-classic-server", "Rust server binary"],
  ["Binary", "Download from GitHub Releases", "Native binary (5 platforms)"],
];

export default function V1ReleaseCompletion() {
  return (
    <ReportShell width="wide" ariaLabel="Velocity v1.0.0 Release Preparation Report">
      <Stack gap="section">
        <Stack gap="component">
          <H1>Velocity v1.0.0 — Release Ready</H1>
          <Text tone="secondary">
            All packages bumped, cross-compile configured, SDK publishing automated
          </Text>
          <MetricsGrid variant="header" columns={5} items={headlineMetrics} />
        </Stack>

        <ReportSection title="Version Bumps" divided description="All packages moved from 0.1.0 to 1.0.0">
          <Table
            headers={["Ecosystem", "Count", "Change", "Details"]}
            rows={versionBumps}
          />
        </ReportSection>

        <ReportSection title="Cross-Compile Matrix" divided description="3 server variants × 5 platforms = 15 native binaries">
          <ChartContainer ariaLabel="Binary targets" caption="15 binaries attached to GitHub Release">
            <BarChart
              categories={platformCategories}
              series={[{ name: "Server Variants", data: binaryCounts }]}
              colorByCategory
            />
          </ChartContainer>
        </ReportSection>

        <ReportSection title="Release Pipeline" divided description="Automated workflow triggered by git tag">
          <Table
            headers={["Step", "Action", "Output"]}
            rows={releasePipeline}
          />
        </ReportSection>

        <ReportSection title="User Installation Methods" divided description="8 ways to install Velocity v1.0.0">
          <Table
            headers={["Method", "Command", "Notes"]}
            rows={installMethods}
          />
        </ReportSection>

        <ReportSection title="Verification Evidence" divided>
          <Stack gap="component">
            <Callout tone="positive">
              17 Cargo.toml + 8 package.json + 2 pyproject.toml + 1 pom.xml + 1 Chart.yaml all at 1.0.0
            </Callout>
            <Callout tone="positive">
              cargo check --workspace passes successfully (zero errors)
            </Callout>
            <Callout tone="positive">
              Zero remaining 0.1.0 references in any package file
            </Callout>
            <Callout tone="positive">
              5 internal crates marked publish=false (bench, dev, test, prod-bench)
            </Callout>
            <Callout tone="positive">
              CHANGELOG.md created with comprehensive v1.0.0 release notes (112 lines)
            </Callout>
          </Stack>
        </ReportSection>

        <ReportSection title="Final Outcome">
          <Callout tone="positive">
            Velocity v1.0.0 is fully prepared for release. Push the v1.0.0 tag and the workflow
            handles everything: 15 native binaries, Docker multi-arch image, npm/PyPI/Maven/Go
            SDK publishing, staging + production deployment, and a GitHub Release with all artifacts.
            Users can install via Docker, Helm, npm, pip, Maven, Go, cargo, or native binary download.
          </Callout>
        </ReportSection>
      </Stack>
    </ReportShell>
  );
}
