"""
Tests for the Velocity Python Migration Tool.

Covers:
- Temporal → Velocity import conversion
- Restate → Velocity import conversion
- DBOS → Velocity import conversion
- Qualified reference conversion
- API call remapping
- Framework auto-detection
- Bare import handling
- Exception handling conversion
- Inter-flavor migration
"""

import re
import sys
import os
import unittest

# Add parent directory to path so we can import the migration tool
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from velocity_sdk.migrate import (
    migrate_file, detect_framework, TEMPORAL_PATTERNS, RESTATE_PATTERNS,
    DBOS_PATTERNS, MigrationPattern
)


class TestTemporalImportConversion(unittest.TestCase):
    """Test Temporal → Velocity import transformations."""

    def test_from_temporalio_import_workflow(self):
        source = "from temporalio import workflow"
        result, _ = migrate_file(source, 'temporal')
        self.assertIn("from velocity_sdk import workflow", result)
        self.assertNotIn("temporalio", result)

    def test_from_temporalio_import_activity(self):
        source = "from temporalio import activity"
        result, _ = migrate_file(source, 'temporal')
        self.assertIn("activity", result)
        self.assertNotIn("temporalio", result)

    def test_from_temporalio_exceptions_submodule(self):
        source = "from temporalio.exceptions import ApplicationError"
        result, _ = migrate_file(source, 'temporal')
        self.assertIn("from velocity_sdk.exceptions import ApplicationError", result)

    def test_bare_import_temporalio_workflow(self):
        source = "import temporalio.workflow"
        result, _ = migrate_file(source, 'temporal')
        self.assertIn("import velocity_sdk.workflow", result)
        self.assertNotIn("temporalio", result)

    def test_bare_import_temporalio_activity(self):
        source = "import temporalio.activity"
        result, _ = migrate_file(source, 'temporal')
        self.assertIn("import velocity_sdk.activity", result)

    def test_bare_import_temporalio_client(self):
        source = "import temporalio.client"
        result, _ = migrate_file(source, 'temporal')
        self.assertIn("import velocity_sdk.client", result)


class TestTemporalQualifiedReferences(unittest.TestCase):
    """Test qualified reference conversion in code body."""

    def test_temporalio_workflow_prefix(self):
        source = "temporalio.workflow.defn"
        result, _ = migrate_file(source, 'temporal')
        self.assertIn("workflow.defn", result)
        self.assertNotIn("temporalio.workflow", result)

    def test_temporalio_converter_prefix(self):
        source = "temporalio.converter.PayloadConverter"
        result, _ = migrate_file(source, 'temporal')
        self.assertIn("workflow.PayloadConverter", result)

    def test_exceptions_application_error(self):
        source = "raise exceptions.ApplicationError('test error')"
        result, _ = migrate_file(source, 'temporal')
        self.assertIn("ApplicationError('test error')", result)
        self.assertNotIn("exceptions.ApplicationError", result)


class TestTemporalAPICalls(unittest.TestCase):
    """Test Temporal API call remapping."""

    def test_workflow_decorator(self):
        source = "@workflow.defn"
        result, _ = migrate_file(source, 'temporal')
        self.assertIn("@workflow.defn", result)

    def test_activity_decorator(self):
        source = "@activity.defn"
        result, _ = migrate_file(source, 'temporal')
        self.assertIn("@activity.defn", result)

    def test_workflow_start_activity(self):
        source = "await workflow.start_activity('MyActivity')"
        result, _ = migrate_file(source, 'temporal')
        self.assertIn("execute_activity", result)

    def test_workflow_wait_condition(self):
        source = "await workflow.wait_condition(lambda: self.ready)"
        result, _ = migrate_file(source, 'temporal')
        self.assertIn("wait_condition", result)

    def test_workflow_start_child_workflow(self):
        source = "result = await workflow.start_child_workflow('ChildWF')"
        result, _ = migrate_file(source, 'temporal')
        self.assertIn("start_child_workflow", result)


class TestRestateConversion(unittest.TestCase):
    """Test Restate → Velocity transformations."""

    def test_restate_context_sleep(self):
        source = "await context.sleep(5)"
        result, _ = migrate_file(source, 'restate')
        self.assertIn("sleep(5)", result)


class TestDBOSConversion(unittest.TestCase):
    """Test DBOS → Velocity transformations."""

    def test_dbos_import(self):
        source = "from dbos import DBOS"
        result, _ = migrate_file(source, 'dbos')
        self.assertIn("from velocity_sdk import", result)

    def test_dbos_sleep(self):
        source = "await DBOS.sleep(10)"
        result, _ = migrate_file(source, 'dbos')
        self.assertIn("sleep(10)", result)

    def test_dbos_recv(self):
        source = "msg = await DBOS.recv()"
        result, _ = migrate_file(source, 'dbos')
        self.assertIn("recv()", result)

    def test_dbos_set_event(self):
        source = "await DBOS.set_event('key', 'value')"
        result, _ = migrate_file(source, 'dbos')
        self.assertIn("set_event", result)


class TestFrameworkDetection(unittest.TestCase):
    """Test auto-detection of source framework."""

    def test_detect_temporal(self):
        content = """
from temporalio import workflow
from temporalio.activity import info

@workflow.defn
class MyWorkflow:
    @workflow.run
    async def run(self):
        pass
"""
        framework, confidence = detect_framework(content)
        self.assertEqual(framework, 'temporal')
        self.assertGreater(confidence, 0.3)

    def test_detect_restate(self):
        content = """
import restate
from restate import Context

async def my_handler(ctx: Context):
    result = await ctx.run('MyActivity')
    return result
"""
        framework, confidence = detect_framework(content)
        self.assertEqual(framework, 'restate')

    def test_detect_dbos(self):
        content = """
from dbos import DBOS

@DBOS.workflow
def my_workflow():
    DBOS.sleep(1)
"""
        framework, confidence = detect_framework(content)
        self.assertEqual(framework, 'dbos')

    def test_detect_empty_content(self):
        framework, confidence = detect_framework("")
        self.assertEqual(confidence, 0.0)


class TestAutoDetect(unittest.TestCase):
    """Test auto-detection mode in migrate_file."""

    def test_auto_detect_temporal(self):
        source = "from temporalio import workflow\n@workflow.defn\nclass WF: pass"
        result, info = migrate_file(source, 'auto')
        self.assertTrue(info.success)
        self.assertEqual(info.detected_framework, 'temporal')
        self.assertNotIn("temporalio", result)

    def test_auto_detect_low_confidence(self):
        source = "# just a comment\nx = 42"
        result, info = migrate_file(source, 'auto')
        self.assertFalse(info.success)


class TestPatternCompleteness(unittest.TestCase):
    """Verify pattern sets have expected coverage."""

    def test_temporal_patterns_count(self):
        self.assertGreater(len(TEMPORAL_PATTERNS), 30,
            "Temporal patterns should have at least 30 patterns")

    def test_restate_patterns_count(self):
        self.assertGreater(len(RESTATE_PATTERNS), 5,
            "Restate patterns should have at least 5 patterns")

    def test_dbos_patterns_count(self):
        self.assertGreater(len(DBOS_PATTERNS), 5,
            "DBOS patterns should have at least 5 patterns")

    def test_all_patterns_have_required_fields(self):
        for name, patterns in [
            ('temporal', TEMPORAL_PATTERNS),
            ('restate', RESTATE_PATTERNS),
            ('dbos', DBOS_PATTERNS),
        ]:
            for p in patterns:
                self.assertIsNotNone(p.name, f"{name} pattern missing name")
                self.assertIsNotNone(p.source_pattern, f"{name} pattern {p.name} missing source_pattern")
                self.assertIsNotNone(p.target_template, f"{name} pattern {p.name} missing target_template")


class TestMigrationResult(unittest.TestCase):
    """Test migration result metadata."""

    def test_successful_migration_has_transformation_count(self):
        source = "from temporalio import workflow\nx = temporalio.workflow.info"
        result, info = migrate_file(source, 'temporal')
        self.assertTrue(info.success)
        self.assertGreater(info.transformations, 0)
        self.assertEqual(info.detected_framework, 'temporal')

    def test_unknown_framework_fails(self):
        source = "x = 1"
        result, info = migrate_file(source, 'unknown_framework')
        self.assertFalse(info.success)
        self.assertIsNotNone(info.error)


if __name__ == '__main__':
    unittest.main(verbosity=2)
