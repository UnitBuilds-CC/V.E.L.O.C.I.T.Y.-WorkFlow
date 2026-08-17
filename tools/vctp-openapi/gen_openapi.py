#!/usr/bin/env python3
"""gen_openapi.py — Generate OpenAPI 3.0 spec from VCTP protocol definitions.

Reads VctpMethods constants and VctpRpcRequest/VctpRpcResponse schemas
to generate a complete openapi.yaml with all VCTP methods as REST endpoints.

Each method maps to POST /vctp/{method-name}.

Usage:
    python gen_openapi.py > openapi.yaml
    python gen_openapi.py --output openapi.yaml
"""

import argparse
import json
import sys
from datetime import datetime

# VCTP method definitions (mirrors VctpMethods in vctp_rpc.rs)
METHODS = {
    # Workflow Lifecycle (100-199)
    "start-workflow": {
        "id": 100,
        "summary": "Start a new workflow execution",
        "tags": ["Workflow Lifecycle"],
        "request_fields": ["namespace", "workflow_id", "workflow_type", "total_steps", "metadata"],
        "response_fields": ["workflow_id", "run_id", "run_status"],
    },
    "signal-workflow": {
        "id": 101,
        "summary": "Send a signal to a running workflow",
        "tags": ["Workflow Lifecycle"],
        "request_fields": ["namespace", "workflow_id", "signal_name", "payload"],
        "response_fields": [],
    },
    "query-workflow": {
        "id": 102,
        "summary": "Query a workflow execution state",
        "tags": ["Workflow Lifecycle"],
        "request_fields": ["namespace", "workflow_id", "query_type"],
        "response_fields": ["run_status"],
    },
    "cancel-workflow": {
        "id": 103,
        "summary": "Request cancellation of a workflow",
        "tags": ["Workflow Lifecycle"],
        "request_fields": ["namespace", "workflow_id"],
        "response_fields": [],
    },
    "terminate-workflow": {
        "id": 104,
        "summary": "Forcefully terminate a workflow",
        "tags": ["Workflow Lifecycle"],
        "request_fields": ["namespace", "workflow_id"],
        "response_fields": [],
    },
    "describe-workflow": {
        "id": 105,
        "summary": "Get detailed information about a workflow",
        "tags": ["Workflow Lifecycle"],
        "request_fields": ["namespace", "workflow_id"],
        "response_fields": ["workflow_id", "run_id", "run_status"],
    },
    "list-workflows": {
        "id": 106,
        "summary": "List workflow executions",
        "tags": ["Workflow Lifecycle"],
        "request_fields": ["namespace", "max_count"],
        "response_fields": ["count"],
    },
    "reset-workflow": {
        "id": 107,
        "summary": "Reset a workflow to a previous state",
        "tags": ["Workflow Lifecycle"],
        "request_fields": ["namespace", "workflow_id"],
        "response_fields": ["workflow_id", "run_id"],
    },
    "update-workflow": {
        "id": 108,
        "summary": "Update a running workflow",
        "tags": ["Workflow Lifecycle"],
        "request_fields": ["namespace", "workflow_id", "update_name", "payload"],
        "response_fields": [],
    },
    "complete-workflow": {
        "id": 109,
        "summary": "Complete a workflow execution",
        "tags": ["Workflow Lifecycle"],
        "request_fields": ["namespace", "workflow_id", "payload"],
        "response_fields": [],
    },
    # Task Dispatch (200-299)
    "poll-workflow-task": {
        "id": 200,
        "summary": "Poll for pending workflow tasks",
        "tags": ["Task Dispatch"],
        "request_fields": ["namespace"],
        "response_fields": ["payload"],
    },
    "poll-activity-task": {
        "id": 201,
        "summary": "Poll for pending activity tasks",
        "tags": ["Task Dispatch"],
        "request_fields": ["namespace"],
        "response_fields": ["payload"],
    },
    "complete-workflow-task": {
        "id": 202,
        "summary": "Complete a workflow task",
        "tags": ["Task Dispatch"],
        "request_fields": ["namespace", "workflow_id", "payload"],
        "response_fields": [],
    },
    "complete-activity-task": {
        "id": 203,
        "summary": "Complete an activity task",
        "tags": ["Task Dispatch"],
        "request_fields": ["namespace", "workflow_id", "payload"],
        "response_fields": [],
    },
    # Namespace Management (300-399)
    "register-namespace": {
        "id": 300,
        "summary": "Register a new namespace",
        "tags": ["Namespace Management"],
        "request_fields": ["namespace"],
        "response_fields": ["run_status"],
    },
    "describe-namespace": {
        "id": 301,
        "summary": "Describe a namespace",
        "tags": ["Namespace Management"],
        "request_fields": ["namespace"],
        "response_fields": ["workflow_id", "run_status"],
    },
    # System (500-599)
    "health-check": {
        "id": 500,
        "summary": "Check server health",
        "tags": ["System"],
        "request_fields": [],
        "response_fields": ["run_status"],
        "no_auth": True,
    },
    "count-workflows": {
        "id": 502,
        "summary": "Count workflow executions",
        "tags": ["System"],
        "request_fields": ["namespace"],
        "response_fields": ["count"],
    },
    "batch-signal": {
        "id": 503,
        "summary": "Send multiple signals to a workflow",
        "tags": ["System"],
        "request_fields": ["namespace", "workflow_id", "signal_name", "signal_count", "payload"],
        "response_fields": ["count"],
    },
    "batch-terminate": {
        "id": 504,
        "summary": "Terminate multiple workflows",
        "tags": ["System"],
        "request_fields": ["namespace", "workflow_id"],
        "response_fields": [],
    },
    # Advanced (600-699)
    "signal-with-start": {
        "id": 606,
        "summary": "Signal a workflow, starting it if not running",
        "tags": ["Advanced"],
        "request_fields": ["namespace", "workflow_id", "workflow_type", "total_steps", "signal_name", "payload"],
        "response_fields": ["workflow_id", "run_id"],
    },
}

# Request field schemas
FIELD_SCHEMAS = {
    "namespace": {"type": "string", "default": "default", "description": "Namespace for the operation"},
    "workflow_id": {"type": "string", "description": "Workflow execution identifier"},
    "workflow_type": {"type": "string", "description": "Workflow type name"},
    "total_steps": {"type": "integer", "format": "uint32", "description": "Total number of workflow steps"},
    "signal_name": {"type": "string", "description": "Name of the signal"},
    "signal_count": {"type": "integer", "format": "uint32", "description": "Number of signals to send"},
    "query_type": {"type": "string", "description": "Type of query to execute"},
    "update_name": {"type": "string", "description": "Name of the update operation"},
    "payload": {"type": "string", "format": "binary", "description": "Binary payload data"},
    "max_count": {"type": "integer", "format": "int64", "description": "Maximum number of results to return"},
    "metadata": {"type": "object", "additionalProperties": {"type": "string"}, "description": "Key-value metadata"},
    "auth_token": {"type": "string", "description": "JWT bearer token for authentication"},
    "api_key": {"type": "string", "description": "API key for authentication"},
    "idempotency_key": {"type": "string", "description": "Idempotency key for duplicate detection"},
}

# Response field schemas
RESPONSE_SCHEMAS = {
    "workflow_id": {"type": "string", "description": "Workflow execution identifier"},
    "run_id": {"type": "string", "description": "Run identifier"},
    "run_status": {"type": "string", "description": "Current workflow status"},
    "count": {"type": "integer", "format": "uint64", "description": "Count result"},
    "payload": {"type": "string", "format": "binary", "description": "Binary response payload"},
}


def generate_openapi():
    """Generate the complete OpenAPI 3.0 specification."""
    spec = {
        "openapi": "3.0.3",
        "info": {
            "title": "VCTP — Velocity Transfer Protocol API",
            "description": (
                "REST API gateway for the Velocity Transfer Protocol (VCTP). "
                "Each endpoint translates HTTP requests to VCTP RPC calls over UDP. "
                "VCTP provides sub-microsecond latency with zero-copy binary framing, "
                "CRC32 integrity, and optional AES-GCM encryption."
            ),
            "version": "1.0.0",
            "contact": {
                "name": "Velocity Team",
            },
            "license": {
                "name": "Proprietary",
            },
        },
        "servers": [
            {
                "url": "http://localhost:8080/api/v1",
                "description": "Local development server",
            },
        ],
        "paths": {},
        "components": {
            "schemas": {
                "VctpRpcRequest": generate_request_schema(),
                "VctpRpcResponse": generate_response_schema(),
                "AuthError": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "integer", "example": 401},
                        "error": {"type": "string", "example": "authentication required"},
                    },
                },
                "RateLimitError": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "integer", "example": 429},
                        "error": {"type": "string", "example": "rate limit exceeded"},
                    },
                },
                "OverloadError": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "integer", "example": 503},
                        "error": {"type": "string", "example": "service overloaded"},
                    },
                },
            },
            "securitySchemes": {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                    "bearerFormat": "JWT",
                    "description": "JWT bearer token authentication",
                },
                "apiKeyAuth": {
                    "type": "apiKey",
                    "in": "header",
                    "name": "X-API-Key",
                    "description": "API key authentication",
                },
                "idempotencyKey": {
                    "type": "apiKey",
                    "in": "header",
                    "name": "Idempotency-Key",
                    "description": "Idempotency key for duplicate request detection",
                },
            },
        },
        "tags": [
            {"name": "Workflow Lifecycle", "description": "Workflow execution management"},
            {"name": "Task Dispatch", "description": "Task polling and completion"},
            {"name": "Namespace Management", "description": "Namespace CRUD operations"},
            {"name": "System", "description": "Health, metrics, and batch operations"},
            {"name": "Advanced", "description": "Advanced workflow operations"},
        ],
    }

    # Generate paths for each method
    for method_name, method_def in METHODS.items():
        path = f"/vctp/{method_name}"
        spec["paths"][path] = generate_path_item(method_name, method_def)

    return spec


def generate_request_schema():
    """Generate the VctpRpcRequest schema."""
    properties = {}
    for field_name, field_def in FIELD_SCHEMAS.items():
        properties[field_name] = dict(field_def)
    properties["method"] = {"type": "integer", "description": "VCTP method identifier"}

    return {
        "type": "object",
        "properties": properties,
        "required": ["method"],
    }


def generate_response_schema():
    """Generate the VctpRpcResponse schema."""
    properties = {
        "status": {"type": "integer", "description": "Status code: 0 = OK, non-zero = error"},
        "sequence": {"type": "integer", "format": "uint64", "description": "Correlates to request sequence"},
        "error": {"type": "string", "nullable": True, "description": "Error message when status != 0"},
    }
    for field_name, field_def in RESPONSE_SCHEMAS.items():
        properties[field_name] = dict(field_def)

    return {
        "type": "object",
        "properties": properties,
        "required": ["status", "sequence"],
    }


def generate_path_item(method_name: str, method_def: dict) -> dict:
    """Generate an OpenAPI path item for a VCTP method."""
    request_props = {}
    for field in method_def.get("request_fields", []):
        if field in FIELD_SCHEMAS:
            request_props[field] = dict(FIELD_SCHEMAS[field])

    # Always include auth fields
    request_props["auth_token"] = dict(FIELD_SCHEMAS["auth_token"])
    request_props["api_key"] = dict(FIELD_SCHEMAS["api_key"])
    request_props["idempotency_key"] = dict(FIELD_SCHEMAS["idempotency_key"])

    response_props = {"status": {"type": "integer"}, "sequence": {"type": "integer"}}
    for field in method_def.get("response_fields", []):
        if field in RESPONSE_SCHEMAS:
            response_props[field] = dict(RESPONSE_SCHEMAS[field])

    # Security
    security = []
    if not method_def.get("no_auth"):
        security = [{"bearerAuth": []}, {"apiKeyAuth": []}]

    return {
        "post": {
            "summary": method_def["summary"],
            "operationId": method_name.replace("-", "_"),
            "tags": method_def.get("tags", ["Uncategorized"]),
            "security": security,
            "requestBody": {
                "required": True,
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "object",
                            "properties": request_props,
                        },
                    },
                },
            },
            "responses": {
                "200": {
                    "description": "Successful response",
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": response_props,
                            },
                        },
                    },
                },
                "401": {
                    "description": "Authentication required",
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/AuthError"},
                        },
                    },
                },
                "429": {
                    "description": "Rate limit exceeded",
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/RateLimitError"},
                        },
                    },
                },
                "503": {
                    "description": "Service overloaded (circuit breaker open)",
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/OverloadError"},
                        },
                    },
                },
            },
            "x-vctp-method-id": method_def["id"],
        },
    }


def to_yaml(spec: dict) -> str:
    """Convert the spec dict to YAML format (simple serializer)."""
    try:
        import yaml
        return yaml.dump(spec, default_flow_style=False, sort_keys=False, allow_unicode=True)
    except ImportError:
        # Fallback: output as JSON if PyYAML not available
        return json.dumps(spec, indent=2, ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser(description="Generate OpenAPI spec for VCTP")
    parser.add_argument("--output", "-o", help="Output file (default: stdout)")
    parser.add_argument("--json", action="store_true", help="Output as JSON instead of YAML")
    args = parser.parse_args()

    spec = generate_openapi()

    if args.json:
        output = json.dumps(spec, indent=2, ensure_ascii=False)
    else:
        output = to_yaml(spec)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output)
        print(f"OpenAPI spec written to {args.output}", file=sys.stderr)
    else:
        print(output)


if __name__ == "__main__":
    main()
