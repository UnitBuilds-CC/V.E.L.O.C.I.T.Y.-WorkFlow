#!/usr/bin/env python3
"""
Example: Simple task worker using the VELOCITY-WorkFlow Python SDK.

Demonstrates:
  - Connecting to the VELOCITY-WorkFlow server
  - Registering for a task queue
  - Polling for tasks in a loop
  - Executing task logic
  - Graceful error handling
  - Graceful shutdown on SIGINT / SIGTERM

Prerequisites:
  1. Start the VELOCITY-WorkFlow server:
     cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run

  2. Install the SDK:
     cd VELOCITY-WorkFlow/sdk/python && pip install -r requirements.txt

  3. Run this worker:
     python examples/simple_worker.py
"""

import os
import signal
import sys
import time
import json
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from velocity_sdk import VelocityClient, WorkflowStatus, VelocityError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("velocity-worker")

# ── Configuration ────────────────────────────────────────────────────────
SERVER_ADDR = "localhost:7234"
TASK_QUEUE = "orders"
POLL_INTERVAL_SEC = 1.0
MAX_RETRIES = 3

# ── Graceful shutdown flag ───────────────────────────────────────────────
shutdown_requested = False


def handle_signal(signum, frame):
    global shutdown_requested
    logger.info("Received signal %s — shutting down gracefully...", signum)
    shutdown_requested = True


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


# ── Task handlers ────────────────────────────────────────────────────────
def process_order(task):
    """Process an incoming order task."""
    payload = json.loads(task.get("input", "{}"))
    order_id = payload.get("order_id", "unknown")
    logger.info("Processing order %s", order_id)
    # Simulate work
    time.sleep(0.05)
    return {"status": "shipped", "order_id": order_id}


TASK_HANDLERS = {
    "order-processing": process_order,
}


# ── Worker loop ──────────────────────────────────────────────────────────
def run_worker():
    logger.info("Starting VELOCITY-WorkFlow Python worker")
    logger.info("Server: %s | Queue: %s", SERVER_ADDR, TASK_QUEUE)

    client = VelocityClient(SERVER_ADDR)

    try:
        logger.info("Worker registered on task queue '%s'", TASK_QUEUE)

        while not shutdown_requested:
            try:
                # Poll for a workflow task from the server
                task = client.poll_task(TASK_QUEUE, timeout_ms=2000)

                if task is None:
                    logger.debug("No task available — retrying")
                    time.sleep(POLL_INTERVAL_SEC)
                    continue

                task_type = task.get("workflow_type", "unknown")
                handler = TASK_HANDLERS.get(task_type)

                if handler is None:
                    logger.warning("No handler for task type '%s' — skipping", task_type)
                    client.fail_task(task["workflow_key"], f"No handler for {task_type}")
                    continue

                # Execute the task
                result = handler(task)
                client.complete_workflow(task["workflow_key"], json.dumps(result).encode())
                logger.info("Task '%s' completed successfully", task_type)

            except VelocityError as exc:
                logger.error("Velocity error while processing task: %s", exc)
                time.sleep(POLL_INTERVAL_SEC)

            except Exception as exc:
                logger.exception("Unexpected error processing task: %s", exc)
                time.sleep(POLL_INTERVAL_SEC)

    except Exception as exc:
        logger.exception("Fatal worker error: %s", exc)
    finally:
        client.close()
        logger.info("Worker shut down cleanly")


if __name__ == "__main__":
    run_worker()
