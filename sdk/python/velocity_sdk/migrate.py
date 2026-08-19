#!/usr/bin/env python3
"""
Velocity Python Migration Tool

Scans a Python codebase for Temporal, Restate, or DBOS workflow patterns
and converts them to Velocity Python SDK workflows.

Usage:
    python -m velocity_sdk.migrate --src ./my_project --from temporal --to velocity
    python -m velocity_sdk.migrate --src ./workflows --from auto --to velocity
    python -m velocity_sdk.migrate --src workflow.py --from restate --to velocity
"""

import argparse
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


# ─── Pattern Definitions ─────────────────────────────────────────────────────

@dataclass
class MigrationPattern:
    """A source pattern and its Velocity replacement."""
    name: str
    source_pattern: re.Pattern
    target_template: str
    source_framework: str  # 'temporal', 'restate', 'dbos', or 'any'


# ─── Temporal → Velocity Patterns ────────────────────────────────────────────

TEMPORAL_PATTERNS: list[MigrationPattern] = [
    # Import replacements
    MigrationPattern(
        name='temporal-import-workflow',
        source_pattern=re.compile(r'from\s+temporalio\s+import\s+workflow'),
        target_template='from velocity_sdk import workflow, WorkflowContext',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-import-activity',
        source_pattern=re.compile(r'from\s+temporalio\s+import\s+activity'),
        target_template='from velocity_sdk import activity, ActivityContext',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-import-client',
        source_pattern=re.compile(r'from\s+temporalio\.client\s+import'),
        target_template='from velocity_sdk.client import VelocityClient',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-import-worker',
        source_pattern=re.compile(r'from\s+temporalio\.worker\s+import'),
        target_template='from velocity_sdk.worker import Worker',
        source_framework='temporal',
    ),
    # Decorator replacements
    MigrationPattern(
        name='temporal-workflow-decorator',
        source_pattern=re.compile(r'@workflow\.run'),
        target_template='@workflow',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-activity-decorator',
        source_pattern=re.compile(r'@activity\.definition'),
        target_template='@activity',
        source_framework='temporal',
    ),
    # Method call replacements
    MigrationPattern(
        name='temporal-execute-activity',
        source_pattern=re.compile(r'await\s+workflow\.execute_activity\s*\(\s*(\w+)\s*,\s*'),
        target_template='await ctx.execute_activity(\\1, ',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-start-activity',
        source_pattern=re.compile(r'await\s+workflow\.start_activity\s*\(\s*(\w+)\s*,\s*'),
        target_template='await ctx.execute_activity(\\1, ',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-sleep',
        source_pattern=re.compile(r'await\s+asyncio\.sleep\s*\('),
        target_template='await ctx.sleep(',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-signal-handler',
        source_pattern=re.compile(r'@workflow\.signal'),
        target_template='@signal',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-query-handler',
        source_pattern=re.compile(r'@workflow\.query'),
        target_template='@query',
        source_framework='temporal',
    ),
    # Class inheritance
    MigrationPattern(
        name='temporal-workflow-class',
        source_pattern=re.compile(r'class\s+(\w+)\s*:\s*#\s*Temporal\s*workflow'),
        target_template='class \\1:  # Velocity workflow',
        source_framework='temporal',
    ),
    # Search attributes
    MigrationPattern(
        name='temporal-search-attributes',
        source_pattern=re.compile(r'workflow\.search_attributes\s*\('),
        target_template='ctx.search_attributes(',
        source_framework='temporal',
    ),
    # Memo
    MigrationPattern(
        name='temporal-memo',
        source_pattern=re.compile(r'workflow\.memo\b'),
        target_template='ctx.memo',
        source_framework='temporal',
    ),
    # Update handler
    MigrationPattern(
        name='temporal-update-handler',
        source_pattern=re.compile(r'@workflow\.update'),
        target_template='@update',
        source_framework='temporal',
    ),
    # Continue-as-new
    MigrationPattern(
        name='temporal-continue-as-new',
        source_pattern=re.compile(r'workflow\.continue_as_new\s*\('),
        target_template='ctx.continue_as_new(',
        source_framework='temporal',
    ),
    # ─── Child Workflow Patterns ─────────────────────────────────────────────
    MigrationPattern(
        name='temporal-execute-child-workflow',
        source_pattern=re.compile(r'await\s+workflow\.execute_child_workflow\s*\(\s*(\w+)'),
        target_template='await ctx.execute_child_workflow(\\1',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-start-child-workflow',
        source_pattern=re.compile(r'await\s+workflow\.start_child_workflow\s*\(\s*(\w+)'),
        target_template='await ctx.start_child_workflow(\\1',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-child-workflow-options',
        source_pattern=re.compile(r'workflow\.ChildWorkflowOptions\s*\('),
        target_template='velocity.ChildWorkflowOptions(',
        source_framework='temporal',
    ),
    # ─── Activity Options Patterns ───────────────────────────────────────────
    MigrationPattern(
        name='temporal-activity-options',
        source_pattern=re.compile(r'workflow\.ActivityOptions\s*\('),
        target_template='velocity.ActivityOptions(',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-execute-local-activity',
        source_pattern=re.compile(r'await\s+workflow\.execute_local_activity\s*\(\s*(\w+)'),
        target_template='await ctx.execute_local_activity(\\1',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-local-activity-options',
        source_pattern=re.compile(r'workflow\.LocalActivityOptions\s*\('),
        target_template='velocity.LocalActivityOptions(',
        source_framework='temporal',
    ),
    # ─── Coroutine & Concurrency Patterns ────────────────────────────────────
    MigrationPattern(
        name='temporal-workflow-create-task',
        source_pattern=re.compile(r'asyncio\.create_task\s*\('),
        target_template='ctx.create_task(',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-workflow-gather',
        source_pattern=re.compile(r'asyncio\.gather\s*\('),
        target_template='ctx.gather(',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-workflow-await',
        source_pattern=re.compile(r'await\s+workflow\.await_\s*\('),
        target_template='await ctx.await_(',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-workflow-await-with-timeout',
        source_pattern=re.compile(r'await\s+workflow\.await_with_timeout\s*\('),
        target_template='await ctx.await_with_timeout(',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-new-future',
        source_pattern=re.compile(r'workflow\.Future\s*\('),
        target_template='ctx.Future(',
        source_framework='temporal',
    ),
    # ─── Relay/Nexus Operation Patterns ──────────────────────────────────────
    MigrationPattern(
        name='temporal-new-nexus-client',
        source_pattern=re.compile(r'workflow\.new_nexus_client\s*\('),
        target_template='ctx.new_relay_client(',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-nexus-execute-operation',
        source_pattern=re.compile(r'await\s+client\.execute_operation\s*\('),
        target_template='await relay_client.execute(',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-nexus-operation-options',
        source_pattern=re.compile(r'workflow\.NexusOperationOptions\s*\('),
        target_template='velocity.RelayOperationOptions(',
        source_framework='temporal',
    ),
    # ─── Activity Context Patterns ───────────────────────────────────────────
    MigrationPattern(
        name='temporal-activity-get-info',
        source_pattern=re.compile(r'activity\.info\s*\('),
        target_template='ctx.info()',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-activity-record-heartbeat',
        source_pattern=re.compile(r'activity\.record_heartbeat\s*\('),
        target_template='ctx.record_heartbeat(',
        source_framework='temporal',
    ),
    # ─── Workflow Context Patterns ───────────────────────────────────────────
    MigrationPattern(
        name='temporal-workflow-get-info',
        source_pattern=re.compile(r'workflow\.info\s*\('),
        target_template='ctx.workflow_info()',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-workflow-get-logger',
        source_pattern=re.compile(r'workflow\.logger\s*\('),
        target_template='ctx.logger()',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-workflow-with-cancel',
        source_pattern=re.compile(r'workflow\.with_cancel\s*\('),
        target_template='ctx.with_cancel(',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-signal-external-workflow',
        source_pattern=re.compile(r'await\s+workflow\.signal_external_workflow\s*\('),
        target_template='await ctx.signal_external_workflow(',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-workflow-get-version',
        source_pattern=re.compile(r'workflow\.get_version\s*\('),
        target_template='ctx.get_version(',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-upsert-search-attributes',
        source_pattern=re.compile(r'await\s+workflow\.upsert_search_attributes\s*\('),
        target_template='await ctx.upsert_search_attributes(',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-upsert-memo',
        source_pattern=re.compile(r'await\s+workflow\.upsert_memo\s*\('),
        target_template='await ctx.upsert_memo(',
        source_framework='temporal',
    ),
    # ─── Error Handling Patterns ─────────────────────────────────────────────
    MigrationPattern(
        name='temporal-new-application-error',
        source_pattern=re.compile(r'workflow\.ApplicationError\s*\('),
        target_template='velocity.ApplicationError(',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-canceled-error',
        source_pattern=re.compile(r'workflow\.CanceledError'),
        target_template='velocity.CanceledError',
        source_framework='temporal',
    ),
    MigrationPattern(
        name='temporal-import-temporal-package',
        source_pattern=re.compile(r'from\s+temporalio\.nexus\s+import'),
        target_template='from velocity_sdk.relay import',
        source_framework='temporal',
    ),
]

# ─── Restate → Velocity Patterns ─────────────────────────────────────────────

RESTATE_PATTERNS: list[MigrationPattern] = [
    MigrationPattern(
        name='restate-import',
        source_pattern=re.compile(r'from\s+restate\s+import\s+'),
        target_template='from velocity_sdk import ',
        source_framework='restate',
    ),
    MigrationPattern(
        name='restate-service-decorator',
        source_pattern=re.compile(r'@restate\.service'),
        target_template='@workflow',
        source_framework='restate',
    ),
    MigrationPattern(
        name='restate-handler',
        source_pattern=re.compile(r'@ctx\.handler'),
        target_template='@activity',
        source_framework='restate',
    ),
    MigrationPattern(
        name='restate-ctx-run',
        source_pattern=re.compile(r'await\s+ctx\.run\s*\(\s*(\w+)\s*,\s*'),
        target_template='await ctx.execute_activity(\\1, ',
        source_framework='restate',
    ),
    MigrationPattern(
        name='restate-ctx-get',
        source_pattern=re.compile(r'await\s+ctx\.get\s*\(\s*[\'"](\w+)[\'"]'),
        target_template='await ctx.get_state(\\1\\1)',
        source_framework='restate',
    ),
    MigrationPattern(
        name='restate-ctx-set',
        source_pattern=re.compile(r'await\s+ctx\.set\s*\(\s*[\'"](\w+)[\'"]\s*,'),
        target_template='await ctx.set_state(\\1\\1,',
        source_framework='restate',
    ),
    MigrationPattern(
        name='restate-ctx-sleep',
        source_pattern=re.compile(r'await\s+ctx\.sleep\s*\('),
        target_template='await ctx.sleep(',
        source_framework='restate',
    ),
    MigrationPattern(
        name='restate-ctx-invoke',
        source_pattern=re.compile(r'await\s+ctx\.invoke\s*\(\s*(\w+)\s*,\s*[\'"]+(\w+)[\'"]+'),
        target_template='await ctx.execute_activity(\\1.\\2',
        source_framework='restate',
    ),
    # Idempotency key
    MigrationPattern(
        name='restate-idempotency-key',
        source_pattern=re.compile(r'ctx\.idempotency_key\b'),
        target_template='ctx.idempotency_key',
        source_framework='restate',
    ),
    # Service client
    MigrationPattern(
        name='restate-service-client',
        source_pattern=re.compile(r'restate\.client\.ServiceClient\s*\('),
        target_template='VelocityClient(',
        source_framework='restate',
    ),
]

# ─── DBOS → Velocity Patterns ────────────────────────────────────────────────

DBOS_PATTERNS: list[MigrationPattern] = [
    MigrationPattern(
        name='dbos-import',
        source_pattern=re.compile(r'from\s+dbos\s+import\s+'),
        target_template='from velocity_sdk import ',
        source_framework='dbos',
    ),
    MigrationPattern(
        name='dbos-workflow-decorator',
        source_pattern=re.compile(r'@DBOS\.workflow'),
        target_template='@workflow',
        source_framework='dbos',
    ),
    MigrationPattern(
        name='dbos-transaction-decorator',
        source_pattern=re.compile(r'@DBOS\.transaction'),
        target_template='@activity',
        source_framework='dbos',
    ),
    MigrationPattern(
        name='dbos-sleep',
        source_pattern=re.compile(r'await\s+DBOS\.sleep\s*\('),
        target_template='await ctx.sleep(',
        source_framework='dbos',
    ),
    MigrationPattern(
        name='dbos-recv',
        source_pattern=re.compile(r'await\s+DBOS\.recv\s*\('),
        target_template='await ctx.recv(',
        source_framework='dbos',
    ),
    MigrationPattern(
        name='dbos-set-event',
        source_pattern=re.compile(r'await\s+DBOS\.set_event\s*\(\s*[\'"](\w+)[\'"]'),
        target_template='await ctx.set_event(\\1\\1)',
        source_framework='dbos',
    ),
    MigrationPattern(
        name='dbos-get-event',
        source_pattern=re.compile(r'await\s+DBOS\.get_event\s*\(\s*[\'"]+(\w+)[\'"]+'),
        target_template='await ctx.get_event(\\1\\1)',
        source_framework='dbos',
    ),
    # Queue operations
    MigrationPattern(
        name='dbos-queue-enqueue',
        source_pattern=re.compile(r'await\s+DBOS\.enqueue\s*\('),
        target_template='await ctx.enqueue(',
        source_framework='dbos',
    ),
    MigrationPattern(
        name='dbos-queue-dequeue',
        source_pattern=re.compile(r'await\s+DBOS\.dequeue\s*\('),
        target_template='await ctx.dequeue(',
        source_framework='dbos',
    ),
    # HTTP handler
    MigrationPattern(
        name='dbos-http-handler',
        source_pattern=re.compile(r'@DBOS\.http_handler\s*\('),
        target_template='@http_handler(',
        source_framework='dbos',
    ),
]

ALL_PATTERNS = TEMPORAL_PATTERNS + RESTATE_PATTERNS + DBOS_PATTERNS

# ─── Inter-Flavor Migration Patterns (Server ↔ Binary ↔ Embedded) ────────────

INTER_FLAVOR_PATTERNS: dict[str, list[MigrationPattern]] = {
    'server→binary': [
        MigrationPattern(name='s2b-import', source_pattern=re.compile(r'from\s+velocity_sdk\s+import'), target_template='from velocity_sdk.binary import', source_framework='server'),
        MigrationPattern(name='s2b-execute-activity', source_pattern=re.compile(r'ctx\.execute_activity\('), target_template='ctx.invoke(', source_framework='server'),
        MigrationPattern(name='s2b-child-workflow', source_pattern=re.compile(r'ctx\.execute_child_workflow\('), target_template='ctx.invoke(', source_framework='server'),
        MigrationPattern(name='s2b-get-signal', source_pattern=re.compile(r'ctx\.get_signal_channel\('), target_template='ctx.promise(', source_framework='server'),
        MigrationPattern(name='s2b-wait-signal', source_pattern=re.compile(r'ctx\.wait_for_signal\('), target_template='ctx.await_condition(', source_framework='server'),
        MigrationPattern(name='s2b-set-state', source_pattern=re.compile(r'ctx\.set_state\('), target_template='ctx.set(', source_framework='server'),
        MigrationPattern(name='s2b-get-state', source_pattern=re.compile(r'ctx\.get_state\('), target_template='ctx.get(', source_framework='server'),
        MigrationPattern(name='s2b-relay-client', source_pattern=re.compile(r'ctx\.new_relay_client\('), target_template='ctx.new_service_client(', source_framework='server'),
    ],
    'server→embedded': [
        MigrationPattern(name='s2e-import', source_pattern=re.compile(r'from\s+velocity_sdk\s+import'), target_template='from velocity_sdk.embedded import', source_framework='server'),
        MigrationPattern(name='s2e-execute-activity', source_pattern=re.compile(r'ctx\.execute_activity\('), target_template='ctx.invoke(', source_framework='server'),
        MigrationPattern(name='s2e-child-workflow', source_pattern=re.compile(r'ctx\.execute_child_workflow\('), target_template='ctx.start_child_workflow(', source_framework='server'),
        MigrationPattern(name='s2e-get-signal', source_pattern=re.compile(r'ctx\.get_signal_channel\('), target_template='ctx.await_signal(', source_framework='server'),
        MigrationPattern(name='s2e-wait-signal', source_pattern=re.compile(r'ctx\.wait_for_signal\('), target_template='ctx.await_condition(', source_framework='server'),
        MigrationPattern(name='s2e-relay-client', source_pattern=re.compile(r'ctx\.new_relay_client\('), target_template='ctx.new_client(', source_framework='server'),
    ],
    'binary→server': [
        MigrationPattern(name='b2s-import', source_pattern=re.compile(r'from\s+velocity_sdk\.binary\s+import'), target_template='from velocity_sdk import', source_framework='binary'),
        MigrationPattern(name='b2s-invoke', source_pattern=re.compile(r'ctx\.invoke\('), target_template='ctx.execute_activity(', source_framework='binary'),
        MigrationPattern(name='b2s-promise', source_pattern=re.compile(r'ctx\.promise\('), target_template='ctx.get_signal_channel(', source_framework='binary'),
        MigrationPattern(name='b2s-set', source_pattern=re.compile(r'ctx\.set\('), target_template='ctx.set_state(', source_framework='binary'),
        MigrationPattern(name='b2s-get', source_pattern=re.compile(r'ctx\.get\('), target_template='ctx.get_state(', source_framework='binary'),
        MigrationPattern(name='b2s-service-client', source_pattern=re.compile(r'ctx\.new_service_client\('), target_template='ctx.new_relay_client(', source_framework='binary'),
    ],
    'binary→embedded': [
        MigrationPattern(name='b2e-import', source_pattern=re.compile(r'from\s+velocity_sdk\.binary\s+import'), target_template='from velocity_sdk.embedded import', source_framework='binary'),
        MigrationPattern(name='b2e-invoke', source_pattern=re.compile(r'ctx\.invoke\('), target_template='ctx.invoke(', source_framework='binary'),
        MigrationPattern(name='b2e-promise', source_pattern=re.compile(r'ctx\.promise\('), target_template='ctx.await_signal(', source_framework='binary'),
        MigrationPattern(name='b2e-set', source_pattern=re.compile(r'ctx\.set\('), target_template='ctx.set_state(', source_framework='binary'),
        MigrationPattern(name='b2e-get', source_pattern=re.compile(r'ctx\.get\('), target_template='ctx.get_state(', source_framework='binary'),
        MigrationPattern(name='b2e-service-client', source_pattern=re.compile(r'ctx\.new_service_client\('), target_template='ctx.new_client(', source_framework='binary'),
    ],
    'embedded→server': [
        MigrationPattern(name='e2s-import', source_pattern=re.compile(r'from\s+velocity_sdk\.embedded\s+import'), target_template='from velocity_sdk import', source_framework='embedded'),
        MigrationPattern(name='e2s-await-signal', source_pattern=re.compile(r'ctx\.await_signal\('), target_template='ctx.get_signal_channel(', source_framework='embedded'),
        MigrationPattern(name='e2s-child-wf', source_pattern=re.compile(r'ctx\.start_child_workflow\('), target_template='ctx.execute_child_workflow(', source_framework='embedded'),
        MigrationPattern(name='e2s-client', source_pattern=re.compile(r'ctx\.new_client\('), target_template='ctx.new_relay_client(', source_framework='embedded'),
    ],
    'embedded→binary': [
        MigrationPattern(name='e2b-import', source_pattern=re.compile(r'from\s+velocity_sdk\.embedded\s+import'), target_template='from velocity_sdk.binary import', source_framework='embedded'),
        MigrationPattern(name='e2b-await-signal', source_pattern=re.compile(r'ctx\.await_signal\('), target_template='ctx.promise(', source_framework='embedded'),
        MigrationPattern(name='e2b-set-state', source_pattern=re.compile(r'ctx\.set_state\('), target_template='ctx.set(', source_framework='embedded'),
        MigrationPattern(name='e2b-get-state', source_pattern=re.compile(r'ctx\.get_state\('), target_template='ctx.get(', source_framework='embedded'),
        MigrationPattern(name='e2b-child-wf', source_pattern=re.compile(r'ctx\.start_child_workflow\('), target_template='ctx.invoke(', source_framework='embedded'),
        MigrationPattern(name='e2b-client', source_pattern=re.compile(r'ctx\.new_client\('), target_template='ctx.new_service_client(', source_framework='embedded'),
    ],
}


def get_inter_flavor_patterns(source: str, target: str) -> list[MigrationPattern]:
    """Get migration patterns for a Velocity flavor-to-flavor migration."""
    key = f'{source}→{target}'
    return INTER_FLAVOR_PATTERNS.get(key, [])


# ─── Framework Detection ─────────────────────────────────────────────────────

def detect_framework(content: str) -> tuple[str, float]:
    """Detect which framework the code uses. Returns (framework, confidence)."""
    scores = {'temporal': 0, 'restate': 0, 'dbos': 0, 'server': 0, 'binary': 0, 'embedded': 0}

    # Temporal indicators
    if re.search(r'from\s+temporalio', content): scores['temporal'] += 3
    if re.search(r'@workflow\.run', content): scores['temporal'] += 2
    if re.search(r'workflow\.execute_activity', content): scores['temporal'] += 2
    if re.search(r'@workflow\.signal', content): scores['temporal'] += 1
    if re.search(r'Temporal.*Client', content): scores['temporal'] += 1
    if re.search(r'workflow\.search_attributes', content): scores['temporal'] += 1
    if re.search(r'@workflow\.update', content): scores['temporal'] += 1
    if re.search(r'workflow\.continue_as_new', content): scores['temporal'] += 1
    if re.search(r'workflow\.memo', content): scores['temporal'] += 1

    # Restate indicators
    if re.search(r'from\s+restate\s+import', content): scores['restate'] += 3
    if re.search(r'@restate\.service', content): scores['restate'] += 2
    if re.search(r'ctx\.run\(', content): scores['restate'] += 1
    if re.search(r'ctx\.invoke\(', content): scores['restate'] += 1
    if re.search(r'ctx\.idempotency_key', content): scores['restate'] += 1
    if re.search(r'ServiceClient', content): scores['restate'] += 1

    # DBOS indicators
    if re.search(r'from\s+dbos\s+import', content): scores['dbos'] += 3
    if re.search(r'@DBOS\.workflow', content): scores['dbos'] += 2
    if re.search(r'@DBOS\.transaction', content): scores['dbos'] += 2
    if re.search(r'DBOS\.sleep', content): scores['dbos'] += 1
    if re.search(r'DBOS\.enqueue', content): scores['dbos'] += 1
    if re.search(r'@DBOS\.http_handler', content): scores['dbos'] += 1

    # Velocity Server indicators
    if re.search(r'from\s+velocity_sdk\s+import', content): scores['server'] += 3
    if re.search(r'ctx\.execute_activity\(', content): scores['server'] += 1
    if re.search(r'ctx\.get_signal_channel\(', content): scores['server'] += 1
    if re.search(r'ctx\.wait_for_signal\(', content): scores['server'] += 1

    # Velocity Binary indicators
    if re.search(r'from\s+velocity_sdk\.binary', content): scores['binary'] += 3
    if re.search(r'ctx\.new_service_client\(', content): scores['binary'] += 1

    # Velocity Embedded indicators
    if re.search(r'from\s+velocity_sdk\.embedded', content): scores['embedded'] += 3
    if re.search(r'ctx\.await_signal\(', content): scores['embedded'] += 1
    if re.search(r'ctx\.start_child_workflow\(', content): scores['embedded'] += 1

    best = max(scores, key=scores.get)
    total = sum(scores.values())
    confidence = scores[best] / total if total > 0 else 0.0
    return best, confidence


# ─── File Migration ──────────────────────────────────────────────────────────

@dataclass
class FileMigrationResult:
    source_path: str
    output_path: Optional[str] = None
    success: bool = True
    error: Optional[str] = None
    detected_framework: str = ''
    transformations: int = 0
    confidence: float = 0.0


def migrate_file(
    content: str,
    source_framework: str,
    file_path: str = '<unknown>',
    target_flavor: str = 'server',
) -> tuple[str, FileMigrationResult]:
    """Migrate a single file's content. Returns (migrated_code, result)."""
    result = FileMigrationResult(source_path=file_path)

    # Auto-detect if needed
    if source_framework == 'auto':
        detected, confidence = detect_framework(content)
        result.detected_framework = detected
        result.confidence = confidence
        if confidence < 0.3:
            result.success = False
            result.error = f'Low confidence detection: {detected} ({confidence:.2f})'
            return content, result
        source_framework = detected
    else:
        result.detected_framework = source_framework

    # Check if this is an inter-flavor migration
    velocity_flavors = {'server', 'binary', 'embedded'}
    if source_framework in velocity_flavors and source_framework != target_flavor:
        patterns = get_inter_flavor_patterns(source_framework, target_flavor)
        if not patterns:
            result.success = False
            result.error = f'No inter-flavor patterns: {source_framework} → {target_flavor}'
            return content, result
        migrated = content
        count = 0
        for pattern in patterns:
            new_text, n = pattern.source_pattern.subn(pattern.target_template, migrated)
            if n > 0:
                migrated = new_text
                count += n
        result.transformations = count
        return migrated, result

    # Select patterns for external framework migrations
    if source_framework == 'temporal':
        patterns = TEMPORAL_PATTERNS
    elif source_framework == 'restate':
        patterns = RESTATE_PATTERNS
    elif source_framework == 'dbos':
        patterns = DBOS_PATTERNS
    else:
        result.success = False
        result.error = f'Unknown source framework: {source_framework}'
        return content, result

    # Apply transformations
    migrated = content
    count = 0
    for pattern in patterns:
        new_text, n = pattern.source_pattern.subn(pattern.target_template, migrated)
        if n > 0:
            migrated = new_text
            count += n

    result.transformations = count

    # Add Velocity import if not present
    if 'from velocity_sdk import' not in migrated:
        migrated = 'from velocity_sdk import workflow, activity, Worker\n' + migrated

    return migrated, result


# ─── Project Scanner ─────────────────────────────────────────────────────────

SKIP_DIRS = {
    'node_modules', '.git', '.venv', 'venv', '__pycache__', 'dist', 'build',
    'target', '.mypy_cache', '.pytest_cache', '.tox', 'site-packages',
}


def scan_python_files(root_dir: str) -> list[str]:
    """Recursively find all .py files in a directory."""
    files = []
    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Skip unwanted directories
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for fname in filenames:
            if fname.endswith('.py'):
                files.append(os.path.join(dirpath, fname))
    return files


def has_workflow_content(content: str) -> bool:
    """Quick check if file contains workflow-related patterns."""
    indicators = [
        r'temporalio', r'restate', r'dbos',
        r'@workflow', r'@activity', r'@DBOS',
        r'async\s+def.*workflow', r'async\s+def.*activity',
        r'execute_activity', r'ctx\.run\(', r'ctx\.invoke\(',
        r'velocity_sdk', r'velocity_sdk\.binary', r'velocity_sdk\.embedded',
    ]
    return any(re.search(p, content) for p in indicators)


# ─── Bulk Migration ──────────────────────────────────────────────────────────

@dataclass
class BulkResult:
    total_files: int = 0
    migrated: int = 0
    failed: int = 0
    skipped: int = 0
    results: list[FileMigrationResult] = field(default_factory=list)


def bulk_migrate(
    source_dir: str,
    output_dir: str,
    source_framework: str = 'auto',
    dry_run: bool = False,
    target_flavor: str = 'server',
) -> BulkResult:
    """Migrate all Python workflow files in a directory."""
    result = BulkResult()

    # Find all Python files
    py_files = scan_python_files(source_dir)
    result.total_files = len(py_files)

    for file_path in py_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            result.failed += 1
            result.results.append(FileMigrationResult(
                source_path=file_path, success=False, error=str(e),
            ))
            continue

        # Skip files without workflow content
        if not has_workflow_content(content):
            result.skipped += 1
            continue

        # Migrate
        migrated_code, file_result = migrate_file(
            content, source_framework, file_path, target_flavor=target_flavor,
        )
        file_result.source_path = os.path.relpath(file_path, source_dir)

        if file_result.success and not dry_run:
            # Compute output path
            rel_path = os.path.relpath(file_path, source_dir)
            out_path = os.path.join(output_dir, rel_path)
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            with open(out_path, 'w', encoding='utf-8') as f:
                f.write(migrated_code)
            file_result.output_path = out_path
            result.migrated += 1
        elif file_result.success:
            result.migrated += 1
        else:
            result.failed += 1

        result.results.append(file_result)

    return result


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Velocity Python Migration Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Migrate a single file
  python -m velocity_sdk.migrate --src workflow.py --from temporal --to velocity

  # Migrate an entire project
  python -m velocity_sdk.migrate --src ./my_project --from auto --to velocity --output ./migrated

  # Detect framework
  python -m velocity_sdk.migrate --detect ./my_project

  # Dry run
  python -m velocity_sdk.migrate --src ./my_project --from auto --to velocity --dry-run
        """,
    )
    parser.add_argument('--src', required=True, help='Source file or directory')
    parser.add_argument('--from', dest='source_framework', default='auto',
                        choices=['temporal', 'restate', 'dbos', 'server', 'binary', 'embedded', 'auto'],
                        help='Source framework (default: auto-detect)')
    parser.add_argument('--to', default='server',
                        choices=['server', 'binary', 'embedded', 'velocity'],
                        help='Target Velocity flavor (default: server)')
    parser.add_argument('--output', '-o', help='Output file or directory')
    parser.add_argument('--dry-run', action='store_true', help='Detect and report without writing')
    parser.add_argument('--detect', action='store_true', help='Detect framework in directory')

    args = parser.parse_args()

    # Mode: detect
    if args.detect:
        if not os.path.isdir(args.src):
            print(f'Error: --detect requires a directory: {args.src}')
            sys.exit(1)
        py_files = scan_python_files(args.src)
        print(f'Scanning {len(py_files)} Python files in {args.src}...')
        for f in py_files:
            try:
                with open(f, 'r', encoding='utf-8') as fh:
                    content = fh.read()
                fw, conf = detect_framework(content)
                if conf > 0.3:
                    print(f'  {os.path.relpath(f, args.src)}: {fw} ({conf:.0%})')
            except Exception:
                pass
        return

    # Mode: single file
    if os.path.isfile(args.src):
        with open(args.src, 'r', encoding='utf-8') as f:
            content = f.read()

        migrated, result = migrate_file(content, args.source_framework, args.src)

        if not result.success:
            print(f'Migration failed: {result.error}', file=sys.stderr)
            sys.exit(1)

        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(migrated)
            print(f'Written to: {args.output}')
        else:
            print(migrated)

        print(f'\nDetected: {result.detected_framework}')
        print(f'Transformations: {result.transformations}')
        return

    # Mode: directory
    if os.path.isdir(args.src):
        output_dir = args.output or os.path.join(args.src, '..', 'velocity-migrated')
        print(f'Scanning: {args.src}')
        print(f'Output: {output_dir if not args.dry_run else "(dry run)"}')
        print(f'Source framework: {args.source_framework}')
        print()

        bulk = bulk_migrate(args.src, output_dir, args.source_framework, args.dry_run, args.to)

        print(f'Results:')
        print(f'  Total files: {bulk.total_files}')
        print(f'  Migrated: {bulk.migrated}')
        print(f'  Failed: {bulk.failed}')
        print(f'  Skipped: {bulk.skipped}')

        for r in bulk.results:
            status = 'OK' if r.success else 'FAIL'
            fw = r.detected_framework or '?'
            print(f'  [{status}] {r.source_path} ({fw}, {r.transformations} changes)')
            if r.error:
                print(f'         Error: {r.error}')
        return

    print(f'Error: {args.src} is not a file or directory', file=sys.stderr)
    sys.exit(1)


if __name__ == '__main__':
    main()
