import { Callout, Card, CardBody, CardHeader, Divider, Grid, H1, H2, H3, Pill, Row, Stack, Stat, Table, Text } from 'qoder/canvas';

export default function OperationalHardeningReport() {
  return (
    <Stack gap={20}>
      <H1>Operational Hardening — Completion Report</H1>
      <Text tone="secondary">Velocity Workflow Engine · August 2026</Text>

      <Divider />

      <Grid columns={4} gap={12}>
        <Stat value="4" label="Items Completed" tone="success" />
        <Stat value="2,302" label="Total Tests" />
        <Stat value="27" label="New Tests Added" tone="success" />
        <Stat value="0" label="Clippy Warnings" tone="success" />
      </Grid>

      <Callout tone="success">
        All 4 operational hardening items completed. Engine now has graceful shutdown, backup/restore, property-based testing, and CI enforcement.
      </Callout>

      <Divider />

      <H2>Changes Delivered</H2>
      <Table
        headers={['#', 'Area', 'What Changed', 'Impact']}
        rows={[
          ['1', 'Graceful Shutdown', 'Drain-based shutdown with 10s deadline, per-component tracking, drain status in session summary', 'Zero in-flight request loss on Ctrl+C'],
          ['2', 'Backup/Restore', 'WAL snapshot() with fsync, list_snapshots(), engine.backup() creates WAL copy + JSON state export', 'Operational recovery from data loss'],
          ['3', 'Property-Based Testing', '11 proptest properties: WAL roundtrip, SlotMap/SlotVec invariants, StringInterner dedup, DashMap consistency', 'Catches edge cases hand-written tests miss'],
          ['4', 'CI Enforcement', 'Dedicated CI steps for fault_injection_tests and proptest_tests — required to pass (no continue-on-error)', 'Regression safety net in every PR'],
        ]}
      />

      <Divider />

      <H2>Modified Files</H2>
      <Table
        headers={['File', 'Change', 'Description']}
        rows={[
          ['velocity-dev-server/src/main.rs', 'Modified', 'Graceful shutdown: drain tracking, 10s deadline, drain status in summary'],
          ['velocity-workflow-engine/src/wal.rs', 'Modified', 'snapshot() and list_snapshots() on WalManager. Fixed clippy map_or.'],
          ['velocity-workflow-engine/src/engine.rs', 'Modified', 'backup() method: WAL snapshot + JSON workflow state export'],
          ['velocity-workflow-engine/Cargo.toml', 'Modified', 'Added proptest = "1" dev-dependency'],
          ['velocity-workflow-engine/tests/proptest_tests.rs', 'New', '11 property-based tests (211 lines)'],
          ['.github/workflows/ci.yml', 'Modified', 'Added fault_injection_tests and proptest_tests as required CI steps'],
        ]}
      />

      <Divider />

      <H2>Test Results</H2>
      <Grid columns={2} gap={16}>
        <Card>
          <CardHeader><H3>Engine — 2,285 tests</H3></CardHeader>
          <CardBody>
            <Stack gap={6}>
              <Row justify="space-between"><Text>Unit tests</Text><Text tone="success">2,038</Text></Row>
              <Row justify="space-between"><Text>Integration</Text><Text tone="success">41</Text></Row>
              <Row justify="space-between"><Text>Scale</Text><Text tone="success">48</Text></Row>
              <Row justify="space-between"><Text>Stress</Text><Text tone="success">20</Text></Row>
              <Row justify="space-between"><Text>Fault injection</Text><Text tone="success">16</Text></Row>
              <Row justify="space-between"><Text>Property-based (NEW)</Text><Text tone="success">11</Text></Row>
              <Row justify="space-between"><Text>Edge case</Text><Text tone="success">20</Text></Row>
              <Row justify="space-between"><Text>Chaos load</Text><Text tone="success">15</Text></Row>
              <Row justify="space-between"><Text>Scenario</Text><Text tone="success">21</Text></Row>
              <Row justify="space-between"><Text>Other suites</Text><Text tone="success">55</Text></Row>
            </Stack>
          </CardBody>
        </Card>
        <Card>
          <CardHeader><H3>Property-Based Tests (NEW)</H3></CardHeader>
          <CardBody>
            <Stack gap={6}>
              <Row justify="space-between"><Text>WAL encode/decode roundtrip</Text><Text tone="success">256 cases</Text></Row>
              <Row justify="space-between"><Text>WAL encoded size formula</Text><Text tone="success">256 cases</Text></Row>
              <Row justify="space-between"><Text>SlotMap insert/get</Text><Text tone="success">256 cases</Text></Row>
              <Row justify="space-between"><Text>SlotMap remove/get</Text><Text tone="success">256 cases</Text></Row>
              <Row justify="space-between"><Text>SlotMap len tracking</Text><Text tone="success">256 cases</Text></Row>
              <Row justify="space-between"><Text>SlotVec push/get</Text><Text tone="success">256 cases</Text></Row>
              <Row justify="space-between"><Text>SlotVec pop_front FIFO</Text><Text tone="success">256 cases</Text></Row>
              <Row justify="space-between"><Text>StringInterner roundtrip</Text><Text tone="success">256 cases</Text></Row>
              <Row justify="space-between"><Text>StringInterner dedup</Text><Text tone="success">256 cases</Text></Row>
              <Row justify="space-between"><Text>StringInterner unique</Text><Text tone="success">256 cases</Text></Row>
              <Row justify="space-between"><Text>DashMap insert/get</Text><Text tone="success">256 cases</Text></Row>
            </Stack>
          </CardBody>
        </Card>
      </Grid>

      <Divider />

      <H2>CI Pipeline Changes</H2>
      <Card>
        <CardBody>
          <Stack gap={8}>
            <Row gap={8}><Pill tone="success">Required</Pill><Text>fault_injection_tests — 16 tests, must pass</Text></Row>
            <Row gap={8}><Pill tone="success">Required</Pill><Text>proptest_tests — 11 tests, must pass</Text></Row>
            <Row gap={8}><Pill tone="warning">Advisory</Pill><Text>Other integration tests — continue-on-error: true</Text></Row>
          </Stack>
        </CardBody>
      </Card>

      <Divider />

      <H2>Cumulative Hardening Summary</H2>
      <Text tone="secondary" size="small">
        Combined with the previous production hardening session (WAL fsync, AES-256-GCM, TLS, auth, versioned WAL format), the engine now has:
      </Text>
      <Grid columns={3} gap={12}>
        <Card>
          <CardHeader><H3>Durability</H3></CardHeader>
          <CardBody>
            <Stack gap={4}>
              <Text size="small">WAL fsync on every mutation</Text>
              <Text size="small">Versioned WAL format with magic bytes</Text>
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
              <Text size="small">AES-256-GCM encryption at rest</Text>
              <Text size="small">TLS via rustls on HTTP API</Text>
              <Text size="small">Bearer token auth on /api/ routes</Text>
              <Text size="small">Tamper-detecting GCM auth tags</Text>
              <Text size="small">Unique nonces per encryption</Text>
            </Stack>
          </CardBody>
        </Card>
        <Card>
          <CardHeader><H3>Operations</H3></CardHeader>
          <CardBody>
            <Stack gap={4}>
              <Text size="small">Graceful shutdown with drain tracking</Text>
              <Text size="small">Property-based fuzz testing</Text>
              <Text size="small">Fault injection test suite</Text>
              <Text size="small">CI enforcement of critical tests</Text>
              <Text size="small">DashMap high-concurrency map</Text>
            </Stack>
          </CardBody>
        </Card>
      </Grid>

      <Text tone="secondary" size="small">
        velocity-workflow-engine 2,285 tests + velocity-workflow-core 17 tests = 2,302 total. 0 failures. 0 clippy warnings.
      </Text>
    </Stack>
  );
}
