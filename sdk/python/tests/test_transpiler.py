"""
VELOCITY-WorkFlow Python SDK — Tests for the AST-based transpiler.
"""

import pytest
from velocity_sdk.transpiler import (
    transpile_python,
    is_temporal_workflow,
    TranspilerConfig,
)


class TestImportRewrite:
    def test_rewrites_temporalio_client_import(self):
        source = "from temporalio.client import Client"
        result = transpile_python(source)
        assert "velocity_sdk.client" in result.code
        assert "temporalio.client" not in result.code
        assert result.stats.imports_rewritten == 1

    def test_rewrites_temporalio_worker_import(self):
        source = "from temporalio.worker import Worker"
        result = transpile_python(source)
        assert "velocity_sdk.worker" in result.code
        assert result.stats.imports_rewritten == 1

    def test_rewrites_temporalio_workflow_import(self):
        source = "from temporalio.workflow import defn"
        result = transpile_python(source)
        assert "velocity_sdk.workflow" in result.code
        assert result.stats.imports_rewritten == 1

    def test_does_not_rewrite_unrelated_imports(self):
        source = "from my_module import MyClass"
        result = transpile_python(source)
        assert "my_module" in result.code
        assert result.stats.imports_rewritten == 0


class TestDecoratorRewrite:
    def test_rewrites_workflow_defn_decorator(self):
        source = """
@workflow.defn
class MyWorkflow:
    async def run(self):
        pass
"""
        result = transpile_python(source)
        assert "velocity_workflow" in result.code
        assert result.stats.decorators_rewritten == 1


class TestMethodCallRewrite:
    def test_rewrites_workflow_sleep(self):
        source = "await workflow.sleep(1000)"
        result = transpile_python(source)
        assert "velocity_sleep" in result.code
        assert result.stats.timer_calls_rewritten == 1

    def test_rewrites_execute_activity(self):
        source = "result = await workflow.execute_activity(my_activity)"
        result = transpile_python(source)
        assert "velocity_execute_activity" in result.code
        assert result.stats.activity_calls_rewritten == 1

    def test_rewrites_workflow_signal(self):
        source = "await workflow.signal('approval', data)"
        result = transpile_python(source)
        assert "velocity_signal" in result.code
        assert result.stats.signal_calls_rewritten == 1

    def test_rewrites_workflow_condition(self):
        source = "await workflow.condition(predicate)"
        result = transpile_python(source)
        assert "velocity_condition" in result.code
        assert result.stats.method_calls_rewritten == 1


class TestVersionGuard:
    def test_injects_version_guard(self):
        source = "x = 1"
        result = transpile_python(source, TranspilerConfig(inject_version_guards=True))
        assert "__VELOCITY_VERSION__" in result.code
        assert result.stats.version_guards_injected == 1

    def test_no_version_guard_when_disabled(self):
        source = "x = 1"
        result = transpile_python(source, TranspilerConfig(inject_version_guards=False))
        assert "__VELOCITY_VERSION__" not in result.code


class TestPhaseTracking:
    def test_tracks_import_phase(self):
        source = "from temporalio.client import Client"
        result = transpile_python(source)
        assert "ImportRewrite" in result.stats.phases

    def test_tracks_timer_phase(self):
        source = "await workflow.sleep(100)"
        result = transpile_python(source)
        assert "TimerRewrite" in result.stats.phases

    def test_tracks_version_guard_phase(self):
        source = "x = 1"
        result = transpile_python(source)
        assert "VersionGuard" in result.stats.phases


class TestEdgeCases:
    def test_handles_syntax_error(self):
        source = "def broken("
        result = transpile_python(source)
        assert len(result.diagnostics) > 0
        assert "Syntax error" in result.diagnostics[0]

    def test_handles_empty_source(self):
        result = transpile_python("")
        assert result.stats.total_nodes_visited >= 0
        assert len(result.diagnostics) == 0


class TestIsTemporalWorkflow:
    def test_detects_temporalio_import(self):
        assert is_temporal_workflow("from temporalio.client import Client") is True

    def test_detects_workflow_defn(self):
        assert is_temporal_workflow("@workflow.defn\nclass Wf: pass") is True

    def test_detects_workflow_sleep(self):
        assert is_temporal_workflow("await workflow.sleep(100)") is True

    def test_detects_execute_activity(self):
        assert is_temporal_workflow("workflow.execute_activity(fn)") is True

    def test_returns_false_for_plain_code(self):
        assert is_temporal_workflow("x = 1\nprint(x)") is False
