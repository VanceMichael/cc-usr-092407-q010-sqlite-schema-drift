"""schema 版本记录、顺序迁移、结构校验与并发选举的正式测试。

覆盖需求：
* 全新库初始化为当前版本；
* 早期服务（v1）留下的库按顺序升级，事件与影响结果完整保留，升级后
  继续幂等处理原事件，重复重启仍然幂等；
* 进程硬崩溃或提交失败（模拟磁盘不足）不会留下半迁移状态，重试成功；
* 版本过新、结构被手工改坏、必要索引缺失、引用完整性损坏都会阻止就绪；
* 多实例同时启动只有一个执行升级，其余等待后复核；
* 诊断输出不包含数据库文件路径。
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from app.config import ROOT, load_airports, load_flights
from app.migrations import (
    CRASH_EXIT_CODE,
    LEGACY_V1_SCHEMA,
    REASON_FOREIGN_KEYS,
    REASON_LOCK_TIMEOUT,
    REASON_MIGRATION_FAILED,
    REASON_MISSING_INDEX,
    REASON_SCHEMA_DRIFT,
    REASON_UNKNOWN_DATABASE,
    REASON_UNSUPPORTED_VERSION,
    SCHEMA_VERSION,
    STATE_BLOCKED,
    STATE_LEGACY,
    STATE_READY,
    bootstrap_database,
    observe_database,
)
from app.repository import Repository
from app.service import DisruptionService

FIXTURES_DIR = ROOT / "fixtures"


def open_connection(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(
        str(path), isolation_level=None, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=FULL")
    return conn


def create_empty_v1(path: Path) -> None:
    conn = sqlite3.connect(str(path), isolation_level=None)
    conn.executescript(LEGACY_V1_SCHEMA)
    conn.close()


def columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}


def index_names(conn: sqlite3.Connection) -> set[str]:
    return {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND name NOT LIKE 'sqlite_%'"
        )
    }


class MigrationFixture:
    """先用当前服务在暂存库生成事件，再复刻成早期 v1 数据库。"""

    def __init__(self, tmp: Path):
        self.tmp = tmp
        self.airports = load_airports(FIXTURES_DIR)
        self.flights = load_flights(FIXTURES_DIR, self.airports)

    def build_legacy_with_event(self, path: Path, payload: dict) -> dict:
        staging = self.tmp / "staging.db"
        repo = Repository(staging)
        service = DisruptionService(repo, self.airports, self.flights)
        result = service.submit_event(payload)
        event_rows = [
            dict(r)
            for r in repo._conn.execute("SELECT * FROM events")  # noqa: SLF001
        ]
        impact_rows = [
            dict(r)
            for r in repo._conn.execute("SELECT * FROM impacts")  # noqa: SLF001
        ]
        repo.close()
        staging.unlink(missing_ok=True)
        for suffix in ("-wal", "-shm"):
            p = Path(str(staging) + suffix)
            p.unlink(missing_ok=True)

        create_empty_v1(path)
        conn = sqlite3.connect(str(path), isolation_level=None)
        v1_cols = [
            "event_id",
            "event_version",
            "event_type",
            "airport_code",
            "effective_from",
            "effective_until",
            "reported_at",
            "supersedes_event_id",
            "reason",
            "payload_json",
        ]
        conn.executemany(
            f"INSERT INTO events ({', '.join(v1_cols)}) "
            f"VALUES ({', '.join('?' for _ in v1_cols)})",
            [tuple(row[c] for c in v1_cols) for row in event_rows],
        )
        impact_cols = [
            "event_id",
            "root_event_id",
            "airport_code",
            "flight_id",
            "flight_number",
            "affected_endpoint",
            "impact_status",
            "overlap_minutes",
            "delay_minutes",
            "proposed_departure",
            "proposed_arrival",
            "passenger_count",
            "crosses_midnight",
        ]
        conn.executemany(
            f"INSERT INTO impacts ({', '.join(impact_cols)}) "
            f"VALUES ({', '.join('?' for _ in impact_cols)})",
            [tuple(row[c] for c in impact_cols) for row in impact_rows],
        )
        conn.commit()
        conn.close()
        return result


def aps_close(**overrides) -> dict:
    payload = {
        "event_id": "evt-close0000001",
        "event_version": 1,
        "event_type": "airport.closed",
        "airport_code": "APS",
        "effective_from": "2026-09-07T15:00:00Z",
        "effective_until": "2026-09-07T19:00:00Z",
        "reported_at": "2026-09-07T14:00:00Z",
        "reason": "volcanic ash",
    }
    payload.update(overrides)
    return payload


class FreshDatabaseTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_empty_database_initialises_at_current_version(self) -> None:
        conn = open_connection(self.tmp / "fresh.db")
        state = bootstrap_database(conn)
        self.assertEqual(state.state, STATE_READY)
        self.assertEqual(state.role, "initialized")
        self.assertEqual(state.current_version, SCHEMA_VERSION)
        self.assertEqual(
            conn.execute(
                "SELECT value FROM schema_meta WHERE key='schema_version'"
            ).fetchone()[0],
            str(SCHEMA_VERSION),
        )
        self.assertIn("idx_events_airport", index_names(conn))
        conn.close()


class LegacyUpgradeTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.fixture = MigrationFixture(self.tmp)
        self.airports = self.fixture.airports
        self.flights = self.fixture.flights

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _open_service(self, path: Path) -> tuple[Repository, DisruptionService]:
        repo = Repository(path)
        return repo, DisruptionService(repo, self.airports, self.flights)

    def test_legacy_v1_is_detected_before_upgrade(self) -> None:
        path = self.tmp / "legacy.db"
        create_empty_v1(path)
        conn = open_connection(path)
        state = observe_database(conn)
        self.assertEqual(state.state, STATE_LEGACY)
        self.assertEqual(state.current_version, 1)
        conn.close()

    def test_upgrade_preserves_events_and_impacts_and_keeps_idempotency(self) -> None:
        path = self.tmp / "legacy.db"
        payload = aps_close()
        original = self.fixture.build_legacy_with_event(path, payload)

        repo = Repository(path)
        self.assertTrue(repo.ready)
        self.assertEqual(repo.migration_state.role, "leader")
        self.assertEqual(len(repo.migration_state.applied_steps), 4)

        service = DisruptionService(repo, self.airports, self.flights)

        # 既有事件与全部影响结果完整保留。
        status = service.event_status(payload["event_id"])
        self.assertEqual(status["event"]["event_id"], payload["event_id"])
        self.assertEqual(len(status["impacts"]), original["impact_count"])
        self.assertEqual(status["impacts"], original["impacts"])
        # replay_count 是新增列，旧数据回填默认 0；created_at 回填 reported_at。
        self.assertEqual(status["processing"]["replay_count"], 0)
        row = repo.get_event_row(payload["event_id"])
        self.assertEqual(row["created_at"], payload["reported_at"])
        self.assertEqual(row["replay_count"], 0)

        # 升级后继续幂等处理原事件：相同内容重试返回原结果。
        replayed = service.submit_event(dict(payload))
        self.assertEqual(replayed["processing_state"], "replayed")
        self.assertEqual(replayed["impacts"], original["impacts"])
        self.assertEqual(
            service.event_status(payload["event_id"])["processing"]["replay_count"], 1
        )
        repo.close()

        # 重复"重启"：第二次引导幂等，幂等处理继续生效。
        for expected_count in (2, 3):
            repo2, service2 = self._open_service(path)
            self.assertEqual(repo2.migration_state.role, "verified")
            self.assertEqual(repo2.migration_state.applied_steps, ())
            again = service2.submit_event(dict(payload))
            self.assertEqual(again["processing_state"], "replayed")
            self.assertEqual(again["impacts"], original["impacts"])
            self.assertEqual(
                service2.event_status(payload["event_id"])["processing"]["replay_count"],
                expected_count,
            )
            repo2.close()

    def test_repeated_bootstrap_is_idempotent_and_verifies(self) -> None:
        path = self.tmp / "legacy.db"
        create_empty_v1(path)
        conn = open_connection(path)
        first = bootstrap_database(conn)
        self.assertEqual(first.state, STATE_READY)
        self.assertEqual(first.role, "leader")
        second = bootstrap_database(conn)
        self.assertEqual(second.state, STATE_READY)
        self.assertEqual(second.role, "verified")
        self.assertEqual(second.applied_steps, ())
        # 结构只包含目标版本该有的形态。
        self.assertEqual(
            columns(conn, "events"),
            {
                "event_id", "event_version", "event_type", "airport_code",
                "effective_from", "effective_until", "reported_at",
                "supersedes_event_id", "reason", "payload_json",
                "replay_count", "created_at",
            },
        )
        conn.close()

    def test_inspect_is_read_only(self) -> None:
        path = self.tmp / "legacy.db"
        create_empty_v1(path)
        conn = open_connection(path)
        state = observe_database(conn)
        self.assertEqual(state.state, STATE_LEGACY)
        conn.close()
        conn = sqlite3.connect(str(path))
        self.assertNotIn("schema_meta", {
            r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        })
        self.assertNotIn("replay_count", columns(conn, "events"))
        conn.close()


class CrashAndFailureTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _legacy_path(self, name: str = "crash.db") -> Path:
        path = self.tmp / name
        create_empty_v1(path)
        return path

    def test_hard_process_exit_mid_migration_leaves_no_half_state(self) -> None:
        path = self._legacy_path()
        env = dict(os.environ)
        env["PYTHONPATH"] = str(ROOT)

        # 在第 2 步之后、提交之前硬退出（os._exit，无 finally/清理）。
        proc = subprocess.run(
            [
                sys.executable, "-m", "app.migrations", "upgrade", str(path),
                "--crash-after-step", "2",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(proc.returncode, CRASH_EXIT_CODE, proc.stderr)

        # 事务未提交：磁盘上的结构完全停留在 v1，没有半迁移状态。
        conn = sqlite3.connect(str(path))
        self.assertEqual(
            columns(conn, "events"),
            {
                "event_id", "event_version", "event_type", "airport_code",
                "effective_from", "effective_until", "reported_at",
                "supersedes_event_id", "reason", "payload_json",
            },
        )
        self.assertNotIn("schema_meta", {
            r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        })
        conn.close()
        observed = observe_database(open_connection(path))
        self.assertEqual(observed.state, STATE_LEGACY)

        # 重新执行升级：全部步骤从头跑一遍并成功。
        conn = open_connection(path)
        state = bootstrap_database(conn)
        self.assertTrue(state.ready)
        self.assertEqual(state.role, "leader")
        self.assertEqual(len(state.applied_steps), 4)
        conn.close()

    def test_commit_failure_rolls_everything_back(self) -> None:
        # 模拟磁盘不足：提交时刻抛 OperationalError，已执行的 DDL 必须随
        # 事务整体回滚，进程可以在同一文件上重试成功。
        path = self._legacy_path("diskfull.db")

        class CommitFailingConnection(sqlite3.Connection):
            fail_commit = False

            def execute(self, sql, *params):
                if self.fail_commit and sql == "COMMIT":
                    raise sqlite3.OperationalError("disk I/O error")
                return super().execute(sql, *params)

        conn = sqlite3.connect(
            str(path), isolation_level=None, factory=CommitFailingConnection
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.fail_commit = True
        state = bootstrap_database(conn)
        self.assertEqual(state.state, STATE_BLOCKED)
        self.assertEqual(state.reason, REASON_MIGRATION_FAILED)
        conn.fail_commit = False

        # 回滚已由 bootstrap 完成；v1 结构原封不动。
        self.assertNotIn("replay_count", columns(conn, "events"))
        self.assertFalse(conn.in_transaction)

        retry = bootstrap_database(conn)
        self.assertTrue(retry.ready)
        self.assertIn("replay_count", columns(conn, "events"))
        conn.close()

    def test_repository_crash_gate_fires_once_across_restarts(self) -> None:
        # 端到端验证持久层的一次性闸门：第一次打开在第 1 步后硬崩溃，
        # 哨兵落盘；带着相同环境变量重新打开时正常完成迁移。
        path = self.tmp / "gated.db"
        create_empty_v1(path)

        env = dict(os.environ)
        env["MIGRATION_CRASH_AFTER_STEP"] = "1"
        env["PYTHONPATH"] = str(ROOT)
        code = (
            "from pathlib import Path; "
            "from app.repository import Repository; "
            f"Repository(Path({str(path)!r}))"
        )
        proc = subprocess.run(
            [sys.executable, "-c", code], env=env, capture_output=True, text=True,
            timeout=30,
        )
        self.assertEqual(proc.returncode, CRASH_EXIT_CODE, proc.stderr)
        self.assertTrue((self.tmp / ".migration-crash-sent").exists())
        # 崩溃发生在提交前：仍然是 v1。
        self.assertEqual(observe_database(open_connection(path)).state, STATE_LEGACY)

        proc2 = subprocess.run(
            [sys.executable, "-c", code], env=env, capture_output=True, text=True,
            timeout=30,
        )
        self.assertEqual(proc2.returncode, 0, proc2.stderr)
        self.assertTrue(observe_database(open_connection(path)).ready)


class BlockedSchemaTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _upgraded(self, name: str = "ok.db") -> Path:
        path = self.tmp / name
        conn = open_connection(path)
        self.assertTrue(bootstrap_database(conn).ready)
        conn.close()
        return path

    def test_version_newer_than_binary_blocks_readiness(self) -> None:
        path = self.tmp / "future.db"
        conn = sqlite3.connect(str(path))
        conn.executescript(
            "CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);"
        )
        conn.execute("INSERT INTO schema_meta VALUES ('schema_version', '99')")
        conn.commit()
        conn.close()

        conn = open_connection(path)
        state = bootstrap_database(conn)
        self.assertEqual(state.state, STATE_BLOCKED)
        self.assertEqual(state.reason, REASON_UNSUPPORTED_VERSION)
        self.assertEqual(state.current_version, 99)
        conn.close()

    def test_hand_modified_missing_column_blocks_readiness(self) -> None:
        path = self._upgraded()
        conn = sqlite3.connect(str(path))
        conn.execute("ALTER TABLE events DROP COLUMN replay_count")
        conn.commit()
        conn.close()

        conn = open_connection(path)
        state = bootstrap_database(conn)
        self.assertEqual(state.state, STATE_BLOCKED)
        self.assertEqual(state.reason, REASON_SCHEMA_DRIFT)
        self.assertTrue(
            any("replay_count" in issue for issue in state.issues), state.issues
        )
        conn.close()

    def test_missing_required_index_blocks_readiness(self) -> None:
        path = self._upgraded()
        conn = sqlite3.connect(str(path))
        conn.execute("DROP INDEX idx_impacts_flight")
        conn.commit()
        conn.close()

        conn = open_connection(path)
        state = bootstrap_database(conn)
        self.assertEqual(state.state, STATE_BLOCKED)
        self.assertEqual(state.reason, REASON_MISSING_INDEX)
        self.assertIn("missing index: idx_impacts_flight", state.issues)
        conn.close()

    def test_multiple_kinds_of_drift_are_all_listed(self) -> None:
        path = self._upgraded()
        conn = sqlite3.connect(str(path))
        conn.execute("DROP INDEX idx_events_airport")
        conn.execute("ALTER TABLE events DROP COLUMN replay_count")
        conn.commit()
        conn.close()

        conn = open_connection(path)
        state = bootstrap_database(conn)
        self.assertEqual(state.reason, REASON_MISSING_INDEX)
        self.assertTrue(any("idx_events_airport" in i for i in state.issues))
        self.assertTrue(any("replay_count" in i for i in state.issues))
        conn.close()

    def test_referential_integrity_violation_blocks_upgrade(self) -> None:
        # v1 库里存在引用不存在事件的 impacts 行（外部键默认关闭时可写入）。
        path = self.tmp / "orphan.db"
        create_empty_v1(path)
        conn = sqlite3.connect(str(path))
        conn.execute(
            "INSERT INTO impacts (event_id, root_event_id, airport_code, flight_id,"
            " flight_number, affected_endpoint, impact_status, overlap_minutes,"
            " delay_minutes, proposed_departure, proposed_arrival, passenger_count,"
            " crosses_midnight) VALUES "
            "('evt-ghost0000001','evt-ghost0000001','APS','GHOST','GX1','origin',"
            "'cancelled',10,NULL,NULL,NULL,1,0)"
        )
        conn.commit()
        conn.close()

        conn = open_connection(path)
        state = bootstrap_database(conn)
        self.assertEqual(state.state, STATE_BLOCKED)
        self.assertEqual(state.reason, REASON_FOREIGN_KEYS)
        # 发现损坏后不得执行任何升级步骤。
        self.assertNotIn("replay_count", columns(conn, "events"))
        conn.close()

    def test_unrecognised_database_blocks(self) -> None:
        path = self.tmp / "strange.db"
        conn = sqlite3.connect(str(path))
        conn.execute("CREATE TABLE unrelated_junk (id INTEGER PRIMARY KEY)")
        conn.commit()
        conn.close()

        conn = open_connection(path)
        state = bootstrap_database(conn)
        self.assertEqual(state.state, STATE_BLOCKED)
        self.assertEqual(state.reason, REASON_UNKNOWN_DATABASE)
        conn.close()

    def test_meta_table_without_version_row_blocks(self) -> None:
        path = self.tmp / "metabroken.db"
        conn = sqlite3.connect(str(path))
        conn.execute("CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT)")
        conn.commit()
        conn.close()
        conn = open_connection(path)
        state = bootstrap_database(conn)
        self.assertEqual(state.state, STATE_BLOCKED)
        conn.close()

    def test_diagnostics_never_contain_database_path(self) -> None:
        secret_dir = self.tmp / "secret-dir-zz99"
        secret_dir.mkdir()
        path = secret_dir / "leak.db"
        conn = open_connection(path)
        self.assertTrue(bootstrap_database(conn).ready)
        conn.close()
        conn = sqlite3.connect(str(path))
        conn.execute("DROP INDEX idx_events_airport")
        conn.commit()
        conn.close()

        conn = open_connection(path)
        state = bootstrap_database(conn)
        rendered = json.dumps(state.public_dict())
        self.assertNotIn("secret-dir-zz99", rendered)
        self.assertNotIn(str(path), rendered)
        conn.close()


class ConcurrentInstancesTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.path = self.tmp / "concurrent.db"
        create_empty_v1(self.path)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_single_leader_others_wait_and_recheck(self) -> None:
        conn_a = open_connection(self.path)
        conn_b = open_connection(self.path)
        conn_c = open_connection(self.path)

        leader_holding = threading.Event()
        release_leader = threading.Event()
        leader_done = threading.Event()
        leader_state: dict = {}

        def hold() -> None:
            leader_holding.set()
            release_leader.wait(5)

        def run_leader() -> None:
            leader_state["state"] = bootstrap_database(
                conn_a, lock_timeout=10, on_before_commit=hold
            )
            leader_done.set()

        thread_a = threading.Thread(target=run_leader)
        thread_a.start()
        self.assertTrue(leader_holding.wait(5), "leader never reached pre-commit hold")

        follower_done = threading.Event()
        follower_state: dict = {}

        def run_follower() -> None:
            follower_state["state"] = bootstrap_database(conn_b, lock_timeout=10)
            follower_done.set()

        thread_b = threading.Thread(target=run_follower)
        thread_b.start()
        # 领导者仍持锁：跟随者必须在等待，不能自行升级。
        self.assertFalse(follower_done.wait(0.5))

        # 第三个实例只给很短预算：拿不到锁也没看到完成 -> 锁超时阻断。
        timed_out = bootstrap_database(conn_c, lock_timeout=0.2)
        self.assertEqual(timed_out.state, STATE_BLOCKED)
        self.assertEqual(timed_out.reason, REASON_LOCK_TIMEOUT)
        self.assertEqual(timed_out.role, "follower")

        release_leader.set()
        self.assertTrue(leader_done.wait(5))
        self.assertTrue(follower_done.wait(5), "follower did not finish after commit")
        thread_a.join()
        thread_b.join()

        self.assertEqual(leader_state["state"].role, "leader")
        self.assertEqual(len(leader_state["state"].applied_steps), 4)
        # 跟随者没有重复执行任何步骤，而是在领导者提交后复核通过。
        self.assertTrue(follower_state["state"].ready)
        self.assertEqual(follower_state["state"].role, "follower")
        self.assertEqual(follower_state["state"].applied_steps, ())

        # 最终结构只有一份，版本正确。
        conn = sqlite3.connect(str(self.path))
        self.assertEqual(
            conn.execute(
                "SELECT value FROM schema_meta WHERE key='schema_version'"
            ).fetchone()[0],
            str(SCHEMA_VERSION),
        )
        conn.close()
        conn_a.close()
        conn_b.close()
        conn_c.close()

    def test_repository_becomes_ready_after_leader_releases_lock(self) -> None:
        # 跟随实例首次引导超时（瞬时阻断）；领导者完成后，就绪探针上的
        # 重试无需重启即可复核就绪。
        holder = open_connection(self.path)
        holder.execute("BEGIN IMMEDIATE")

        repo = Repository(self.path, migration_lock_timeout=0.2)
        self.assertFalse(repo.ready)
        self.assertEqual(repo.migration_state.reason, REASON_LOCK_TIMEOUT)

        def finish_leader() -> None:
            time.sleep(0.5)
            holder.execute("ROLLBACK")
            state = bootstrap_database(holder)
            assert state.ready

        thread = threading.Thread(target=finish_leader)
        thread.start()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if repo.ready:
                break
            time.sleep(0.1)
        thread.join()
        self.assertTrue(repo.ready)
        self.assertEqual(repo.migration_state.role, "follower")
        repo.close()
        holder.close()


if __name__ == "__main__":
    unittest.main()
