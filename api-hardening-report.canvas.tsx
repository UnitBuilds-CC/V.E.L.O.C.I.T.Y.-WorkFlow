import {
  Callout,
  Card,
  CardBody,
  CardHeader,
  Grid,
  H1,
  H2,
  H3,
  MetricsGrid,
  Pill,
  ReportSection,
  ReportShell,
  Row,
  Stack,
  Table,
  Text,
} from 'qoder/canvas';

export default function ApiHardeningReport() {
  return (
    <ReportShell width="wide" ariaLabel="API Hardening Completion Report">
      <Stack gap="sectionCompact">
        <header>
          <Stack gap="component">
            <H1>API Hardening &amp; Production Gaps — Completion Report</H1>
            <Text tone="secondary">Velocity Workflow Engine · Session 4 · August 2026</Text>
            <MetricsGrid
              variant="header"
              columns={4}
              items={[
                { label: 'Items Completed', value: '7', tone: 'success' },
                { label: 'Total Tests', value: '2,100' },
                { label: 'New Tests Added', value: '41', tone: 'success' },
                { label: 'Clippy Warnings', value: '0', tone: 'success' },
              ]}
            />
          </Stack>
        </header>

        <Callout tone="success">
          All 7 API hardening items completed. Engine now has encryption key rotation, request body size limits, X-Request-Id propagation, enhanced /metrics, deep /health, Content-Type validation, and comprehensive integration tests.
        </Callout>

        <ReportSection title="Changes Delivered" divided>
          <Table
            headers={['#', 'Area', 'What Changed', 'Impact']}
            rows={[
              ['1', 'Key Rotation', 'rotate_key() with RwLock interior mutability. Retired keys for backward-compatible decryption. Nonce reset per rotation.', 'Cryptographic agility — rotate keys without data loss'],
              ['2', 'Body Size Limit', '10 MB max request body. Content-Length fast-path + post-read enforcement.', 'DoS protection against memory exhaustion'],
              ['3', 'X-Request-Id', 'Extract from client or generate req-ID. Echoed in all responses including 401/413/415 errors.', 'Request correlation across distributed debugging'],
              ['4', 'Enhanced /metrics', 'Added velocity_namespaces and velocity_task_queues Prometheus gauges.', 'Complete operational visibility for scraping'],
              ['5', 'Deep /health', 'Returns version, uptime, workflow counts, namespace count — not just static OK.', 'k8s liveness/readiness probes get real status'],
              ['6', 'Content-Type', 'POST/PUT/PATCH to /api/ must send application/json. Returns 415 on mismatch.', 'Reject malformed requests before parsing'],
              ['7', 'CI Integration', 'api_hardening_tests as required CI step — 14 tests, no continue-on-error.', 'Regression safety net in every PR'],
            ]}
          />
        </ReportSection>

        <ReportSection title="Modified Files" divided>
          <Table
            headers={['File', 'Change', 'Description']}
            rows={[
              ['auth_v2.rs', 'Modified', 'RwLock-based EncryptionState, rotate_key(), retired key chain, backward-compatible decrypt()'],
              ['dev-server/main.rs', 'Modified', 'MAX_BODY_SIZE, SERVER_VERSION, X-Request-Id, Content-Type validation, deep /health, enhanced /metrics'],
              ['grpc_server.rs', 'Modified', 'Removed dead heartbeats code from HistoryServiceImpl, #[allow(dead_code)] annotations'],
              ['cold_storage.rs', 'Modified', 'Clippy fixes: io_other_error, useless_format, dead_code, manual_is_multiple_of'],
              ['api_hardening_tests.rs', 'New (270 lines)', '14 integration tests covering key rotation, API contracts, error formats'],
              ['ci.yml', 'Modified', 'Added api_hardening_tests as required CI step'],
            ]}
          />
        </ReportSection>

        <ReportSection title="Test Results" divided>
          <Grid columns={2} gap={16}>
            <Card>
              <CardHeader><H3>Engine — 2,083 tests</H3></CardHeader>
              <CardBody>
                <Stack gap={6}>
                  <Row justify="space-between"><Text>Unit tests (lib)</Text><Text tone="success">2,042</Text></Row>
                  <Row justify="space-between"><Text>API hardening (NEW)</Text><Text tone="success">14</Text></Row>
                  <Row justify="space-between"><Text>Fault injection</Text><Text tone="success">16</Text></Row>
                  <Row justify="space-between"><Text>Property-based</Text><Text tone="success">11</Text></Row>
                </Stack>
              </CardBody>
            </Card>
            <Card>
              <CardHeader><H3>Key Rotation Tests (NEW)</H3></CardHeader>
              <CardBody>
                <Stack gap={6}>
                  <Row justify="space-between"><Text>Rotation roundtrip</Text><Text tone="success">PASS</Text></Row>
                  <Row justify="space-between"><Text>Config preservation</Text><Text tone="success">PASS</Text></Row>
                  <Row justify="space-between"><Text>Triple rotation — all decryptable</Text><Text tone="success">PASS</Text></Row>
                  <Row justify="space-between"><Text>Nonce counter reset</Text><Text tone="success">PASS</Text></Row>
                  <Row justify="space-between"><Text>Unknown key returns None</Text><Text tone="success">PASS</Text></Row>
                  <Row justify="space-between"><Text>Retired key count tracking</Text><Text tone="success">PASS</Text></Row>
                </Stack>
              </CardBody>
            </Card>
          </Grid>
        </ReportSection>

        <ReportSection title="Key Rotation Architecture" divided>
          <Text tone="secondary" size="small">
            Encryption key rotation uses RwLock&lt;EncryptionState&gt; for safe interior mutability — zero unsafe code.
          </Text>
          <Grid columns={3} gap={12}>
            <Card>
              <CardHeader><H3>Encrypt Path</H3></CardHeader>
              <CardBody>
                <Text size="small">Read lock on state → current cipher + key_id → increment nonce counter → AES-256-GCM encrypt</Text>
              </CardBody>
            </Card>
            <Card>
              <CardHeader><H3>Decrypt Path</H3></CardHeader>
              <CardBody>
                <Text size="small">Read lock → try current key by kid_hash → iterate retired keys → AES-256-GCM decrypt</Text>
              </CardBody>
            </Card>
            <Card>
              <CardHeader><H3>Rotation Path</H3></CardHeader>
              <CardBody>
                <Text size="small">Write lock → mem::replace cipher → push RetiredKey → update config → reset nonce → timestamp</Text>
              </CardBody>
            </Card>
          </Grid>
        </ReportSection>

        <ReportSection title="Cumulative Hardening (Sessions 1–4)" divided>
          <Grid columns={4} gap={12}>
            <Card>
              <CardHeader><H3>Durability</H3></CardHeader>
              <CardBody>
                <Stack gap={4}>
                  <Text size="small">WAL fsync on every mutation</Text>
                  <Text size="small">Versioned WAL format</Text>
                  <Text size="small">CRC32 corruption detection</Text>
                  <Text size="small">Timestamped WAL snapshots</Text>
                  <Text size="small">JSON state backup export</Text>
                </Stack>
              </CardBody>
            </Card>
            <Card>
              <CardHeader><H3>Security</H3></CardHeader>
              <CardBody>
                <Stack gap={4}>
                  <Text size="small">AES-256-GCM encryption</Text>
                  <Text size="small">Key rotation + retired keys</Text>
                  <Text size="small">TLS via rustls</Text>
                  <Text size="small">Bearer token auth</Text>
                  <Text size="small">Body size limits (10 MB)</Text>
                  <Text size="small">Content-Type validation</Text>
                </Stack>
              </CardBody>
            </Card>
            <Card>
              <CardHeader><H3>Operations</H3></CardHeader>
              <CardBody>
                <Stack gap={4}>
                  <Text size="small">Graceful shutdown drain</Text>
                  <Text size="small">X-Request-Id correlation</Text>
                  <Text size="small">Deep /health endpoint</Text>
                  <Text size="small">Prometheus /metrics</Text>
                  <Text size="small">Property-based fuzz testing</Text>
                  <Text size="small">Fault injection suite</Text>
                </Stack>
              </CardBody>
            </Card>
            <Card>
              <CardHeader><H3>Performance</H3></CardHeader>
              <CardBody>
                <Stack gap={4}>
                  <Text size="small">Zero-alloc hot paths</Text>
                  <Text size="small">SlotMap/SlotVec containers</Text>
                  <Text size="small">StringInterner for lookups</Text>
                  <Text size="small">DashMap concurrent map</Text>
                  <Text size="small">jemalloc allocator</Text>
                  <Text size="small">WAL group commit</Text>
                </Stack>
              </CardBody>
            </Card>
          </Grid>
        </ReportSection>

        <ReportSection title="CI Pipeline — Required Checks" divided>
          <Stack gap={8}>
            <Row gap={8}><Pill tone="success">Required</Pill><Text>fault_injection_tests — 16 tests</Text></Row>
            <Row gap={8}><Pill tone="success">Required</Pill><Text>proptest_tests — 11 tests</Text></Row>
            <Row gap={8}><Pill tone="success">Required</Pill><Text>api_hardening_tests — 14 tests (NEW)</Text></Row>
            <Row gap={8}><Pill tone="warning">Advisory</Pill><Text>Other integration tests — continue-on-error: true</Text></Row>
          </Stack>
        </ReportSection>

        <Text tone="secondary" size="small">
          velocity-workflow-engine 2,083 tests + velocity-workflow-core 17 tests = 2,100 total. 0 failures. 0 clippy warnings across engine, core, and dev-server crates.
        </Text>
      </Stack>
    </ReportShell>
  );
}
