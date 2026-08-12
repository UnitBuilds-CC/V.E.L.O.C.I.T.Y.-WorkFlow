"""
Tests for storage backends, journal replay, and crash recovery.
"""

import asyncio
import os
import shutil
import tempfile

import pytest

from velocity_runtime import (
    VirtualObject,
    Service,
    Workflow,
    ObjectContext,
    Context,
    WorkflowContext,
    RuntimeServer,
    ServerConfig,
    InMemoryStorage,
    FileStorage,
    StoredJournal,
    StoredKeyState,
    app,
)


# ═══════════════════════════════════════════════════════════════════════════════
# InMemoryStorage
# ═══════════════════════════════════════════════════════════════════════════════


class TestInMemoryStorage:
    def test_save_and_load_journal(self):
        storage = InMemoryStorage()
        journal = StoredJournal(
            invocation_id="inv-1",
            service_name="Chat",
            handler_name="message",
            key="user-42",
            entries=[{"seq": 0, "type": "run", "output": "hello"}],
            object_state={"count": 5},
            output="hello",
            state="completed",
            created_at=1000.0,
            completed_at=1001.0,
        )
        storage.save_journal(journal)

        loaded = storage.load_journal("inv-1")
        assert loaded is not None
        assert loaded.invocation_id == "inv-1"
        assert loaded.service_name == "Chat"
        assert loaded.key == "user-42"
        assert len(loaded.entries) == 1
        assert loaded.object_state == {"count": 5}
        assert loaded.output == "hello"
        assert loaded.state == "completed"

    def test_load_nonexistent_journal(self):
        storage = InMemoryStorage()
        assert storage.load_journal("nonexistent") is None

    def test_load_journals_for_key(self):
        storage = InMemoryStorage()
        storage.save_journal(StoredJournal(
            invocation_id="inv-1", service_name="Chat", handler_name="msg",
            key="user-1", entries=[], state="completed",
        ))
        storage.save_journal(StoredJournal(
            invocation_id="inv-2", service_name="Chat", handler_name="msg",
            key="user-1", entries=[], state="completed",
        ))
        storage.save_journal(StoredJournal(
            invocation_id="inv-3", service_name="Chat", handler_name="msg",
            key="user-2", entries=[], state="completed",
        ))

        journals = storage.load_journals_for_key("Chat/user-1")
        assert len(journals) == 2

    def test_load_all_journals(self):
        storage = InMemoryStorage()
        storage.save_journal(StoredJournal(
            invocation_id="inv-1", service_name="A", handler_name="h",
            key="", entries=[], state="completed",
        ))
        storage.save_journal(StoredJournal(
            invocation_id="inv-2", service_name="B", handler_name="h",
            key="", entries=[], state="completed",
        ))

        all_journals = storage.load_all_journals()
        assert len(all_journals) == 2

    def test_save_and_load_key_state(self):
        storage = InMemoryStorage()
        storage.save_key_state(StoredKeyState(
            full_key="Chat/user-1",
            state={"history": ["hello", "world"]},
            updated_at=1000.0,
        ))

        loaded = storage.load_key_state("Chat/user-1")
        assert loaded is not None
        assert loaded.state == {"history": ["hello", "world"]}

    def test_load_nonexistent_key_state(self):
        storage = InMemoryStorage()
        assert storage.load_key_state("nonexistent") is None

    def test_delete_journal(self):
        storage = InMemoryStorage()
        storage.save_journal(StoredJournal(
            invocation_id="inv-1", service_name="A", handler_name="h",
            key="", entries=[], state="completed",
        ))
        assert storage.load_journal("inv-1") is not None

        storage.delete_journal("inv-1")
        assert storage.load_journal("inv-1") is None

    def test_clear(self):
        storage = InMemoryStorage()
        storage.save_journal(StoredJournal(
            invocation_id="inv-1", service_name="A", handler_name="h",
            key="", entries=[], state="completed",
        ))
        storage.save_key_state(StoredKeyState(
            full_key="Chat/user-1", state={"x": 1},
        ))

        storage.clear()
        assert len(storage.load_all_journals()) == 0
        assert storage.load_key_state("Chat/user-1") is None


# ═══════════════════════════════════════════════════════════════════════════════
# FileStorage
# ═══════════════════════════════════════════════════════════════════════════════


class TestFileStorage:
    def setup_method(self):
        self.test_dir = tempfile.mkdtemp()

    def teardown_method(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_save_and_load_journal(self):
        storage = FileStorage(self.test_dir)
        journal = StoredJournal(
            invocation_id="inv-1",
            service_name="Chat",
            handler_name="message",
            key="user-42",
            entries=[{"seq": 0, "type": "run"}],
            object_state={"count": 5},
            output="hello",
            state="completed",
            created_at=1000.0,
            completed_at=1001.0,
        )
        storage.save_journal(journal)

        loaded = storage.load_journal("inv-1")
        assert loaded is not None
        assert loaded.invocation_id == "inv-1"
        assert loaded.service_name == "Chat"
        assert loaded.output == "hello"
        assert loaded.state == "completed"

    def test_persistence_across_instances(self):
        """FileStorage persists data across different instances."""
        storage1 = FileStorage(self.test_dir)
        storage1.save_journal(StoredJournal(
            invocation_id="inv-1", service_name="Chat", handler_name="msg",
            key="user-1", entries=[], output="hello", state="completed",
        ))

        # Create a new instance pointing to the same directory
        storage2 = FileStorage(self.test_dir)
        loaded = storage2.load_journal("inv-1")
        assert loaded is not None
        assert loaded.output == "hello"

    def test_save_and_load_key_state(self):
        storage = FileStorage(self.test_dir)
        storage.save_key_state(StoredKeyState(
            full_key="Chat/user-1",
            state={"history": ["hello"]},
        ))

        loaded = storage.load_key_state("Chat/user-1")
        assert loaded is not None
        assert loaded.state == {"history": ["hello"]}

    def test_load_all_journals_from_disk(self):
        storage = FileStorage(self.test_dir)
        for i in range(3):
            storage.save_journal(StoredJournal(
                invocation_id=f"inv-{i}", service_name="Svc", handler_name="h",
                key="", entries=[], state="completed",
            ))

        all_journals = storage.load_all_journals()
        assert len(all_journals) == 3

    def test_delete_journal_from_disk(self):
        storage = FileStorage(self.test_dir)
        storage.save_journal(StoredJournal(
            invocation_id="inv-1", service_name="Svc", handler_name="h",
            key="", entries=[], state="completed",
        ))
        storage.delete_journal("inv-1")
        assert storage.load_journal("inv-1") is None

    def test_clear_disk_storage(self):
        storage = FileStorage(self.test_dir)
        storage.save_journal(StoredJournal(
            invocation_id="inv-1", service_name="Svc", handler_name="h",
            key="", entries=[], state="completed",
        ))
        storage.save_key_state(StoredKeyState(full_key="k1", state={}))

        storage.clear()
        assert len(storage.load_all_journals()) == 0
        assert storage.load_key_state("k1") is None


# ═══════════════════════════════════════════════════════════════════════════════
# Journal Replay
# ═══════════════════════════════════════════════════════════════════════════════


class TestJournalReplay:
    @pytest.mark.asyncio
    async def test_replay_restores_key_state(self):
        """After restart, Virtual Object state is restored from storage."""
        storage = InMemoryStorage()

        # Pre-populate storage with a completed journal containing state
        storage.save_journal(StoredJournal(
            invocation_id="inv-1",
            service_name="Counter",
            handler_name="increment",
            key="counter-1",
            entries=[{"seq": 0, "type": "run", "output": 1}],
            object_state={"count": 42},
            output=42,
            state="completed",
            created_at=1000.0,
            completed_at=1001.0,
        ))

        # Create a new server with the same storage — should replay
        server = RuntimeServer(storage=storage)

        # Register the Counter object
        counter = VirtualObject("Counter")

        @counter.handler()
        async def increment(ctx: ObjectContext, _input):
            count = await ctx.get("count") or 0
            count += 1
            await ctx.set("count", count)
            return count

        server.register(counter)

        # The key state should have been restored from storage
        full_key = "Counter/counter-1"
        assert server._key_state[full_key] == {"count": 42}

    @pytest.mark.asyncio
    async def test_replay_restores_invocation_records(self):
        """After restart, invocation records are restored from storage."""
        storage = InMemoryStorage()
        storage.save_journal(StoredJournal(
            invocation_id="inv-abc",
            service_name="Svc",
            handler_name="handler",
            key="",
            entries=[],
            output="result",
            state="completed",
            created_at=1000.0,
            completed_at=1001.0,
        ))

        server = RuntimeServer(storage=storage)

        # The invocation record should be restored
        inv = server.get_invocation("inv-abc")
        assert inv is not None
        assert inv.service_name == "Svc"
        assert inv.output_data == "result"
        assert inv.state == "completed"

    @pytest.mark.asyncio
    async def test_journal_persisted_on_completion(self):
        """When an invocation completes, its journal is persisted to storage."""
        storage = InMemoryStorage()
        server = RuntimeServer(storage=storage)

        svc = Service("TestSvc")

        @svc.handler()
        async def handler(ctx: Context, input_data):
            result = await ctx.run(lambda: input_data.upper())
            return result

        server.register(svc)

        inv_id = await server.invoke("TestSvc", "handler", input_data="hello")
        # Wait for async execution
        await asyncio.sleep(0.1)

        inv = server.get_invocation(inv_id)
        assert inv.state == "completed"

        # Journal should be persisted to storage
        stored = storage.load_journal(inv_id)
        assert stored is not None
        assert stored.state == "completed"
        assert stored.output == "HELLO"

    @pytest.mark.asyncio
    async def test_key_state_persisted_for_virtual_objects(self):
        """Virtual Object state is persisted to storage after each invocation."""
        storage = InMemoryStorage()
        server = RuntimeServer(storage=storage)

        counter = VirtualObject("Counter")

        @counter.handler()
        async def increment(ctx: ObjectContext, _input):
            count = await ctx.get("count") or 0
            count += 1
            await ctx.set("count", count)
            return count

        server.register(counter)

        inv_id = await server.invoke("Counter", "increment", key="c1")
        await asyncio.sleep(0.1)

        # Key state should be persisted
        key_state = storage.load_key_state("Counter/c1")
        assert key_state is not None
        assert key_state.state["count"] == 1

    @pytest.mark.asyncio
    async def test_failed_journal_persisted(self):
        """Failed invocations also persist their journals for audit."""
        storage = InMemoryStorage()
        server = RuntimeServer(storage=storage)

        svc = Service("FailSvc")

        @svc.handler()
        async def handler(ctx: Context, input_data):
            raise ValueError("intentional failure")

        server.register(svc)

        inv_id = await server.invoke("FailSvc", "handler", input_data="test")
        await asyncio.sleep(0.1)

        inv = server.get_invocation(inv_id)
        assert inv.state == "failed"

        # Failed journal should be persisted
        stored = storage.load_journal(inv_id)
        assert stored is not None
        assert stored.state == "failed"
        assert stored.error is not None

    @pytest.mark.asyncio
    async def test_file_storage_end_to_end(self):
        """FileStorage persists journals and restores them across server restarts."""
        test_dir = tempfile.mkdtemp()
        try:
            # First server instance
            storage1 = FileStorage(test_dir)
            server1 = RuntimeServer(storage=storage1)

            counter = VirtualObject("Counter")

            @counter.handler()
            async def increment(ctx: ObjectContext, _input):
                count = await ctx.get("count") or 0
                count += 1
                await ctx.set("count", count)
                return count

            server1.register(counter)

            inv_id = await server1.invoke("Counter", "increment", key="c1")
            await asyncio.sleep(0.1)

            inv = server1.get_invocation(inv_id)
            assert inv.state == "completed"

            # Second server instance — should replay from disk
            storage2 = FileStorage(test_dir)
            server2 = RuntimeServer(storage=storage2)

            # Key state should be restored
            assert server2._key_state["Counter/c1"]["count"] == 1

            # Invocation record should be restored
            restored = server2.get_invocation(inv_id)
            assert restored is not None
            assert restored.state == "completed"
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_app_factory_with_storage(self):
        """The app() factory accepts a storage parameter."""
        storage = InMemoryStorage()
        svc = Service("TestSvc")

        @svc.handler()
        async def handler(ctx: Context, input_data):
            return input_data

        server = app(services=[svc], storage=storage)
        assert server._storage is storage
