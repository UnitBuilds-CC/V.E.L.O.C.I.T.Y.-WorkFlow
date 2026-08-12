"""Worker for executing workflows and activities in V.E.L.O.C.I.T.Y.-WorkFlow Python SDK"""

import asyncio
import logging
import signal
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from .connection import Connection
from .workflow import get_workflow
from .activity import get_activity

logger = logging.getLogger(__name__)


@dataclass
class WorkerOptions:
    """Options for creating a Worker"""
    host_port: str = "localhost:7233"
    namespace: str = "default"
    task_queue: str = ""
    workflows: Dict[str, Callable] = field(default_factory=dict)
    activities: Dict[str, Callable] = field(default_factory=dict)
    max_concurrent_workflow_tasks: int = 10
    max_concurrent_activity_tasks: int = 10


class Worker:
    """Worker that polls for and executes workflow and activity tasks"""

    def __init__(self, options: WorkerOptions):
        if not options.task_queue:
            raise ValueError("task_queue is required")

        self.options = options
        self.connection = Connection(options.host_port, False)
        self.connection.connect()
        self.running = False
        self.stop_event = threading.Event()

        # Register workflows and activities
        for name, func in options.workflows.items():
            from .workflow import register_workflow
            register_workflow(name, func)

        for name, func in options.activities.items():
            from .activity import register_activity
            register_activity(name, func)

    def run(self) -> None:
        """Start the worker and block until stopped"""
        if self.running:
            raise RuntimeError("Worker is already running")

        self.running = True
        logger.info(f"Worker started for task queue: {self.options.task_queue}")

        # Set up signal handlers for graceful shutdown
        original_sigint = signal.getsignal(signal.SIGINT)
        original_sigterm = signal.getsignal(signal.SIGTERM)

        def signal_handler(signum, frame):
            logger.info("Received shutdown signal")
            self.stop()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            # Start polling threads
            workflow_thread = threading.Thread(target=self._poll_workflow_tasks)
            activity_thread = threading.Thread(target=self._poll_activity_tasks)

            workflow_thread.start()
            activity_thread.start()

            # Wait for stop signal
            self.stop_event.wait()

            # Wait for threads to finish
            workflow_thread.join()
            activity_thread.join()

        finally:
            # Restore signal handlers
            signal.signal(signal.SIGINT, original_sigint)
            signal.signal(signal.SIGTERM, original_sigterm)

            self.connection.close()
            logger.info("Worker stopped")

    def stop(self) -> None:
        """Stop the worker"""
        if not self.running:
            return

        logger.info("Worker stopping...")
        self.stop_event.set()
        self.running = False

    def is_running(self) -> bool:
        """Check if worker is running"""
        return self.running

    def get_task_queue(self) -> str:
        """Get the task queue name"""
        return self.options.task_queue

    def _poll_workflow_tasks(self) -> None:
        """Poll for workflow tasks"""
        while not self.stop_event.is_set():
            try:
                # In a real implementation, this would call the gRPC client
                # to poll for workflow tasks
                self.stop_event.wait(timeout=1.0)
            except Exception as e:
                logger.error(f"Error polling workflow task: {e}")
                self.stop_event.wait(timeout=1.0)

    def _poll_activity_tasks(self) -> None:
        """Poll for activity tasks"""
        while not self.stop_event.is_set():
            try:
                # In a real implementation, this would call the gRPC client
                # to poll for activity tasks
                self.stop_event.wait(timeout=1.0)
            except Exception as e:
                logger.error(f"Error polling activity task: {e}")
                self.stop_event.wait(timeout=1.0)

    def _handle_workflow_task(self, task: Any) -> None:
        """Handle a workflow task"""
        # In a real implementation, this would:
        # 1. Extract workflow type and input from the task
        # 2. Look up the registered workflow function
        # 3. Execute the workflow
        # 4. Send the result back via gRPC
        logger.info(f"Handling workflow task: {task}")

    def _handle_activity_task(self, task: Any) -> None:
        """Handle an activity task"""
        # In a real implementation, this would:
        # 1. Extract activity type and input from the task
        # 2. Look up the registered activity function
        # 3. Execute the activity
        # 4. Send the result back via gRPC
        logger.info(f"Handling activity task: {task}")
