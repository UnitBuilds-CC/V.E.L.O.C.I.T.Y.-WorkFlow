"""
VELOCITY-WorkFlow Python SDK — AST-based transpiler.

Transforms Temporal Python workflow source code into VELOCITY-compatible code
using Python's built-in `ast` module for proper AST-level transformations.
This replaces regex-based approaches with real Python AST manipulation,
avoiding false matches in string literals and comments.
"""

import ast
import textwrap
from typing import List, Optional, Tuple, Dict, Any
from dataclasses import dataclass, field


@dataclass
class TranspilerConfig:
    """Configuration for the Python AST transpiler."""
    velocity_module: str = "velocity_sdk"
    inject_version_guards: bool = True
    rewrite_timers: bool = True
    rewrite_signals: bool = True
    rewrite_activities: bool = True


@dataclass
class TranspileStats:
    """Statistics from a transpilation run."""
    imports_rewritten: int = 0
    decorators_rewritten: int = 0
    method_calls_rewritten: int = 0
    timer_calls_rewritten: int = 0
    signal_calls_rewritten: int = 0
    activity_calls_rewritten: int = 0
    version_guards_injected: int = 0
    total_nodes_visited: int = 0
    phases: List[str] = field(default_factory=list)


@dataclass
class TranspileResult:
    """Result of transpiling a Python workflow file."""
    code: str
    stats: TranspileStats
    diagnostics: List[str]


# ─── AST Transformer ──────────────────────────────────────────────────────────

class VelocityASTTransformer(ast.NodeTransformer):
    """
    AST transformer that rewrites Temporal Python patterns to VELOCITY equivalents.

    Transformations:
    1. temporalio.client → velocity_sdk.client
    2. temporalio.worker → velocity_sdk.worker
    3. temporalio.workflow → velocity_sdk.workflow
    4. @workflow.defn → @velocity_workflow
    5. workflow.sleep() → velocity_sleep()
    6. workflow.signal() → velocity_signal()
    7. workflow.execute_activity() → velocity_execute_activity()
    """

    TEMPORAL_MODULES = {
        "temporalio.client": "velocity_sdk.client",
        "temporalio.worker": "velocity_sdk.worker",
        "temporalio.workflow": "velocity_sdk.workflow",
        "temporalio.activity": "velocity_sdk.activity",
        "temporalio": "velocity_sdk",
    }

    def __init__(self, config: TranspilerConfig):
        self.config = config
        self.stats = TranspileStats()

    def visit_ImportFrom(self, node: ast.ImportFrom) -> ast.AST:
        """Rewrite Temporal imports to VELOCITY imports."""
        self.stats.total_nodes_visited += 1

        if node.module and node.module in self.TEMPORAL_MODULES:
            self.stats.imports_rewritten += 1
            if "ImportRewrite" not in self.stats.phases:
                self.stats.phases.append("ImportRewrite")

            node.module = self.TEMPORAL_MODULES[node.module]

        return node

    def visit_Import(self, node: ast.Import) -> ast.AST:
        """Rewrite top-level Temporal imports."""
        self.stats.total_nodes_visited += 1

        for alias in node.names:
            if alias.name in self.TEMPORAL_MODULES:
                self.stats.imports_rewritten += 1
                if "ImportRewrite" not in self.stats.phases:
                    self.stats.phases.append("ImportRewrite")
                alias.name = self.TEMPORAL_MODULES[alias.name]

        return node

    def visit_ClassDef(self, node: ast.ClassDef) -> ast.AST:
        """Rewrite @workflow.defn decorators to @velocity_workflow."""
        self.stats.total_nodes_visited += 1

        new_decorators = []
        for dec in node.decorator_list:
            if isinstance(dec, ast.Attribute):
                # @workflow.defn → @velocity_workflow
                if isinstance(dec.value, ast.Name) and dec.value.id == "workflow" and dec.attr == "defn":
                    new_dec = ast.Name(id="velocity_workflow", ctx=ast.Load())
                    self.stats.decorators_rewritten += 1
                    if "DecoratorRewrite" not in self.stats.phases:
                        self.stats.phases.append("DecoratorRewrite")
                    new_decorators.append(new_dec)
                else:
                    new_decorators.append(dec)
            else:
                new_decorators.append(dec)

        node.decorator_list = new_decorators
        self.generic_visit(node)
        return node

    def visit_Call(self, node: ast.Call) -> ast.AST:
        """Rewrite Temporal API calls to VELOCITY equivalents."""
        self.stats.total_nodes_visited += 1
        self.generic_visit(node)

        if isinstance(node.func, ast.Attribute):
            attr_name = node.func.attr

            # workflow.sleep() → velocity_sleep()
            if attr_name == "sleep" and self.config.rewrite_timers:
                if isinstance(node.func.value, ast.Name) and node.func.value.id == "workflow":
                    self.stats.timer_calls_rewritten += 1
                    if "TimerRewrite" not in self.stats.phases:
                        self.stats.phases.append("TimerRewrite")
                    return ast.Call(
                        func=ast.Name(id="velocity_sleep", ctx=ast.Load()),
                        args=node.args,
                        keywords=node.keywords,
                    )

            # workflow.execute_activity() → velocity_execute_activity()
            if attr_name == "execute_activity" and self.config.rewrite_activities:
                if isinstance(node.func.value, ast.Name) and node.func.value.id == "workflow":
                    self.stats.activity_calls_rewritten += 1
                    if "ActivityRewrite" not in self.stats.phases:
                        self.stats.phases.append("ActivityRewrite")
                    return ast.Call(
                        func=ast.Name(id="velocity_execute_activity", ctx=ast.Load()),
                        args=node.args,
                        keywords=node.keywords,
                    )

            # workflow.signal() → velocity_signal()
            if attr_name == "signal" and self.config.rewrite_signals:
                if isinstance(node.func.value, ast.Name) and node.func.value.id == "workflow":
                    self.stats.signal_calls_rewritten += 1
                    if "SignalRewrite" not in self.stats.phases:
                        self.stats.phases.append("SignalRewrite")
                    return ast.Call(
                        func=ast.Name(id="velocity_signal", ctx=ast.Load()),
                        args=node.args,
                        keywords=node.keywords,
                    )

            # workflow.condition() → velocity_condition()
            if attr_name == "condition":
                if isinstance(node.func.value, ast.Name) and node.func.value.id == "workflow":
                    self.stats.method_calls_rewritten += 1
                    if "ConditionRewrite" not in self.stats.phases:
                        self.stats.phases.append("ConditionRewrite")
                    return ast.Call(
                        func=ast.Name(id="velocity_condition", ctx=ast.Load()),
                        args=node.args,
                        keywords=node.keywords,
                    )

        return node

    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.AST:
        """Track function definitions."""
        self.stats.total_nodes_visited += 1
        self.generic_visit(node)
        return node

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> ast.AST:
        """Track async function definitions."""
        self.stats.total_nodes_visited += 1
        self.generic_visit(node)
        return node


# ─── Public API ───────────────────────────────────────────────────────────────

def transpile_python(
    source: str,
    config: Optional[TranspilerConfig] = None,
) -> TranspileResult:
    """
    Transpile Temporal Python workflow source code into VELOCITY-compatible code.

    Uses Python's ast module for proper AST-level transformations that avoid
    false matches in string literals and comments.

    Args:
        source: Python source code containing Temporal workflow patterns.
        config: Optional transpiler configuration.

    Returns:
        TranspileResult with transformed code, stats, and diagnostics.
    """
    if config is None:
        config = TranspilerConfig()

    stats = TranspileStats()
    diagnostics: List[str] = []

    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        diagnostics.append(f"Syntax error: {e}")
        return TranspileResult(code=source, stats=stats, diagnostics=diagnostics)

    # Apply AST transformations
    transformer = VelocityASTTransformer(config)
    tree = transformer.visit(tree)
    ast.fix_missing_locations(tree)
    stats = transformer.stats

    # Convert back to source code
    output_code = ast.unparse(tree)

    # Add version guard if configured
    if config.inject_version_guards:
        version_guard = "# VELOCITY Version Guard — auto-injected by transpiler\n__VELOCITY_VERSION__ = 1\n\n"
        output_code = version_guard + output_code
        stats.version_guards_injected = 1
        if "VersionGuard" not in stats.phases:
            stats.phases.append("VersionGuard")

    # Deduplicate phases
    stats.phases = list(dict.fromkeys(stats.phases))

    return TranspileResult(code=output_code, stats=stats, diagnostics=diagnostics)


def is_temporal_workflow(source: str) -> bool:
    """
    Check if a source file contains Temporal-specific patterns.

    Args:
        source: Python source code to check.

    Returns:
        True if the source appears to be a Temporal workflow.
    """
    return (
        "temporalio" in source or
        "@workflow.defn" in source or
        "workflow.sleep" in source or
        "execute_activity" in source
    )
