"""schema 版本记录、顺序迁移、崩溃恢复、并发协调与漂移阻断测试。"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path

from app.migrations import (
    CURRENT_SCHEMA_VERSION,
    MigrationTimeoutError,
    SchemaDriftError,
    SchemaVersionTooNewError,
    prepare_database,
)
from app.repository import Repository
from tests.support import base_event

ROOT = Path(__file__).resolve().parent.parent

# --------------------------------------------------------------------------- #
# 旧版结构（模拟滚动发布前各历史版本留下的文件）
# --------------------------------------------------------------------------- #

_EVENTS_COMMON = """
CREATE TABLE events (
    event_id             TEXT PRIMARY KEY,
    event_version        INTEGER NOT NULL,
    event_type           TEXT NOT NULL,
    airport_code         TEXT NOT NULL,
    effective_from       TEXT NOT NULL,
    effective_until      TEXT,
    reported_at          TEXT NOT NULL,
    supersedes_event_id  TEXT,
    reason               TEXT,
    payload_json         TEXT NOT NULL,
    {replay}
    created_at           TEXT NOT NULL
)
"""

_IMPACTS_OLD = """
CREATE TABLE impacts (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id           TEXT NOT NULL REFERENCES events(event_id),
    root_event_id      TEXT NOT NULL,
    airport_code       TEXT NOT NULL,
    flight_id          TEXT NOT NULL,
    flight_number      TEXT NOT NULL,
    affected_endpoint  TEXT NOT NULL,
    impact_status      TEXT NOT NULL,
    overlap_minutes    INTEGER,
    delay_minutes      INTEGER,
    proposed_departure TEXT,
    proposed_arrival   TEXT,
    UNIQUE(event_id, flight_id, airport_code)
)
"""

_IMPACTS_V3 = """
CREATE TABLE impacts (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id           TEXT NOT NULL REFERENCES events(event_id),
    root_event_id      TEXT NOT NULL,
    airport_code       TEXT NOT NULL,
    flight_id          TEXT NOT NULL,
    flight_number      TEXT NOT NULL,
    affected_endpoint  TEXT NOT NULL,
    impact_status      TEXT NOT NULL,
    overlap_minutes    INTEGER,
    delay_minutes      INTEGER,
    proposed_departure TEXT,
    proposed_arrival   TEXT,
    passenger_count    INTEGER NOT NULL,
    crosses_midnight   INTEGER NOT NULL,
    UNIQUE(event_id, flight_id, airport_code)
)
"""

_INDEXES = (
    "CREATE INDEX idx_impacts_root    ON impacts(root_event_id)",
    "CREATE INDEX idx_impacts_airport ON impacts(airport_code, impact_status)",
    "CREATE INDEX idx_impacts_flight  ON impacts(flight_id)",
    "CREATE INDEX idx_events_airport  ON events(airport_code, event_version)",
)


def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _seed_event(conn: sqlite3.Connection, *, version: int) -> None:
    payload = base_event()
    # 与服务端 DisruptionEvent.to_dict() 的规范化形态保持一致，使旧数据在
    # 升级后仍能走幂等重放（相同载荷比较）。
    payload["supersedes_event_id"] = None
    cols = [
        "event_id", "event_version", "event_type", "airport_code",
        "effective_from", "effective_until", "reported_at",
        "supersedes_event_id", "reason", "payload_json", "created_at",
    ]
    values = [
        payload["event_id"], 1, payload["event_type"], payload["airport_code"],
        payload["effective_from"], payload["effective_until"],
        payload["reported_at"], None, payload["reason"],
        json.dumps(payload, sort_keys=True), _utcnow(),
    ]
    if version >= 2:
        cols.insert(len(cols) - 1, "replay_count")
        values.insert(len(values) - 1, 7)
    placeholders = ", ".join("?" for _ in values)
    conn.execute(
        f"INSERT INTO events ({', '.join(cols)}) VALUES ({placeholders})", values
    )

    impact_common = dict(
        event_id=payload["event_id"],
        root_event_id=payload["event_id"],
        airport_code="APS",
        flight_id="AX-410-20260907",
        flight_number="AX410",
        affected_endpoint="departure",
        impact_status="cancelled",
        overlap_minutes=210,
        delay_minutes=None,
        proposed_departure=None,
        proposed_arrival=None,
    )
    if version >= 3:
        cols_i = list(impact_common) + ["passenger_count", "crosses_midnight"]
        vals_i = list(impact_common.values()) + [210, 1]
    else:
        cols_i = list(impact_common)
        vals_i = list(impact_common.values())
    placeholders = ", ".join("?" for _ in vals_i)
    conn.execute(
        f"INSERT INTO impacts ({', '.join(cols_i)}) VALUES ({placeholders})", vals_i
    )


def build_legacy_db(path: Path, version: int, *, indexes: bool = True) -> None:
    conn = sqlite3.connect(str(path))
    try:
        replay = "replay_count INTEGER NOT NULL DEFAULT 0," if version >= 2 else ""
        conn.execute(_EVENTS_COMMON.format(replay=replay))
        conn.execute(_IMPACTS_V3 if version >= 3 else _IMPACTS_OLD)
        if indexes:
            for stmt in _INDEXES:
                conn.execute(stmt)
        _seed_event(conn, version=version)
        conn.commit()
    finally:
        conn.close()


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


class MigrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.path = Path(self._tmp.name) / "legacy.db"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_fresh_database_initializes_at_current_version(self) -> None:
        result = prepare_database(self.path)
        self.assertEqual(result.schema_version, CURRENT_SCHEMA_VERSION)
        self.assertEqual(result.from_version, 0)
        self.assertEqual(result.actions, ["initialized"])
        with _connect(self.path) as conn:
            version = conn.execute(
                "SELECT schema_version FROM schema_meta WHERE id=1"
            ).fetchone()[0]
            self.assertEqual(version, CURRENT_SCHEMA_VERSION)
            rows = conn.execute(
                "SELECT version, action FROM schema_migrations ORDER BY version"
            ).fetchall()
            self.assertEqual([r["version"] for r in rows], [1, 2, 3])
            self.assertTrue(all(r["action"] == "baseline" for r in rows))

    def test_repeated_open_is_idempotent(self) -> None:
        prepare_database(self.path)
        result = prepare_database(self.path)
        self.assertEqual(result.actions, ["verified"])
        with _connect(self.path) as conn:
            count = conn.execute("SELECT COUNT(*) c FROM schema_migrations").fetchone()["c"]
            self.assertEqual(count, 3)  # 没有重复插入历史

    def test_upgrade_v1_preserves_events_and_impacts(self) -> None:
        self._assert_upgrade_preserves(1, expect_backfill=True, replay_before=0)

    def test_upgrade_v2_preserves_events_and_impacts(self) -> None:
        self._assert_upgrade_preserves(2, expect_backfill=True, replay_before=7)

    def test_upgrade_unversioned_v3_preserves_data(self) -> None:
        # 现行结构但没有版本记录（典型的“老进程建表、新进程接入”）。
        self._assert_upgrade_preserves(3, expect_backfill=False, replay_before=7)

    def _assert_upgrade_preserves(
        self, legacy: int, *, expect_backfill: bool, replay_before: int
    ) -> None:
        build_legacy_db(self.path, legacy)
        result = prepare_database(self.path)
        self.assertEqual(result.schema_version, CURRENT_SCHEMA_VERSION)
        self.assertEqual(result.from_version, legacy)

        with _connect(self.path) as conn:
            event = conn.execute(
                "SELECT * FROM events WHERE event_id=?", (base_event()["event_id"],)
            ).fetchone()
            impact = conn.execute(
                "SELECT * FROM impacts WHERE event_id=?", (base_event()["event_id"],)
            ).fetchone()
            self.assertIsNotNone(event)
            self.assertIsNotNone(impact)
            self.assertEqual(event["event_type"], "airport.closed")
            self.assertEqual(event["replay_count"], replay_before)
            self.assertEqual(impact["overlap_minutes"], 210)
            self.assertEqual(impact["impact_status"], "cancelled")
            if expect_backfill:
                self.assertEqual(impact["passenger_count"], 0)
                self.assertEqual(impact["crosses_midnight"], 0)
            else:
                self.assertEqual(impact["passenger_count"], 210)
                self.assertEqual(impact["crosses_midnight"], 1)
            # 四个必要索引必须就位。
            have = {
                r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='index'"
                )
            }
            for name in (
                "idx_impacts_root", "idx_impacts_airport",
                "idx_impacts_flight", "idx_events_airport",
            ):
                self.assertIn(name, have)
            history = conn.execute(
                "SELECT version, action FROM schema_migrations ORDER BY version"
            ).fetchall()
            self.assertEqual([r["version"] for r in history], [1, 2, 3])
            actions = [r["action"] for r in history]
            if legacy == 1:
                self.assertEqual(actions, ["baseline", "migrate", "migrate"])
            elif legacy == 2:
                self.assertEqual(actions, ["baseline", "baseline", "migrate"])
            else:
                self.assertEqual(actions, ["baseline", "baseline", "baseline"])

        # 升级后 Repository 可直接打开并查询到原有结果。
        repo = Repository(self.path)
        try:
            rows = repo.latest_impacts()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["flight_id"], "AX-410-20260907")
        finally:
            repo.close()

    # ------------------------------------------------------------------ #
    # 中断 / 崩溃
    # ------------------------------------------------------------------ #

    def test_failed_step_rolls_back_leaving_no_half_migration(self) -> None:
        build_legacy_db(self.path, 1)

        def fail_on_step_3(version, conn):
            if version == 3:
                raise sqlite3.OperationalError("simulated disk full")

        with self.assertRaises(sqlite3.OperationalError):
            prepare_database(self.path, failure_hook=fail_on_step_3)

        with _connect(self.path) as conn:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )}
            # 版本表与中间 impacts_new 都不应留下。
            self.assertNotIn("schema_meta", tables)
            self.assertNotIn("impacts_new", tables)
            event_cols = {r[1] for r in conn.execute("PRAGMA table_info(events)")}
            # 整事务回滚：仍是干净的 v1，replay_count 不应出现。
            self.assertNotIn("replay_count", event_cols)
            impact_cols = {r[1] for r in conn.execute("PRAGMA table_info(impacts)")}
            self.assertNotIn("passenger_count", impact_cols)
            count = conn.execute("SELECT COUNT(*) c FROM impacts").fetchone()["c"]
            self.assertEqual(count, 1)  # 数据未丢

        # 修复后重试必须成功且仍然幂等。
        prepare_database(self.path)
        prepare_database(self.path)
        with _connect(self.path) as conn:
            version = conn.execute(
                "SELECT schema_version FROM schema_meta WHERE id=1"
            ).fetchone()[0]
            self.assertEqual(version, CURRENT_SCHEMA_VERSION)
            self.assertEqual(
                conn.execute("SELECT COUNT(*) c FROM impacts").fetchone()["c"], 1
            )

    def test_hard_process_crash_recovers_and_retries(self) -> None:
        build_legacy_db(self.path, 1)

        # 子进程在步骤 2 之后、提交之前被硬杀死（模拟断电/SIGKILL/磁盘满
        # 导致进程退出）。
        crash_script = f"""
import os, sqlite3
from app.migrations import prepare_database
def hook(version, conn):
    if version == 2:
        os._exit(42)
prepare_database({str(self.path)!r}, failure_hook=hook)
"""
        proc = subprocess.run(
            [sys.executable, "-c", crash_script],
            cwd=ROOT,
            capture_output=True,
            timeout=60,
        )
        self.assertEqual(proc.returncode, 42, proc.stderr.decode())

        # 父进程重新打开：未提交的升级被 SQLite 日志回滚，仍是干净的 v1。
        with _connect(self.path) as conn:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )}
            self.assertNotIn("schema_meta", tables)
            event_cols = {r[1] for r in conn.execute("PRAGMA table_info(events)")}
            self.assertNotIn("replay_count", event_cols)

        prepare_database(self.path)
        with _connect(self.path) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT schema_version FROM schema_meta WHERE id=1"
                ).fetchone()[0],
                CURRENT_SCHEMA_VERSION,
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) c FROM impacts").fetchone()["c"], 1
            )

    # ------------------------------------------------------------------ #
    # 并发
    # ------------------------------------------------------------------ #

    def test_concurrent_prepare_single_executor(self) -> None:
        build_legacy_db(self.path, 1)
        n = 8
        barrier = threading.Barrier(n)
        results: list = [None] * n
        errors: list = []

        def worker(i: int) -> None:
            try:
                barrier.wait()
                results[i] = prepare_database(self.path, timeout=30)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        self.assertEqual(errors, [])
        self.assertTrue(all(r.schema_version == CURRENT_SCHEMA_VERSION for r in results))
        # 恰好一个实例执行了 [2,3] 两步；其余只复核。
        executors = [r for r in results if r.migrated_steps == [2, 3]]
        verifiers = [r for r in results if r.migrated_steps == []]
        self.assertEqual(len(executors), 1)
        self.assertEqual(len(verifiers), n - 1)

        with _connect(self.path) as conn:
            # 历史只被写一次。
            rows = conn.execute(
                "SELECT version, COUNT(*) c FROM schema_migrations GROUP BY version"
            ).fetchall()
            self.assertEqual([(r["version"], r["c"]) for r in rows], [(1, 1), (2, 1), (3, 1)])

    def test_waiter_times_out_when_migration_never_finishes(self) -> None:
        # 合法的 v1 库被另一个连接长期占着写锁，等待者应在超时后得到明确
        # 异常（MigrationTimeoutError），而不是假装就绪。
        build_legacy_db(self.path, 1)
        holder = sqlite3.connect(str(self.path), timeout=0)
        holder.execute("PRAGMA journal_mode=WAL")
        holder.commit()
        holder.execute("BEGIN IMMEDIATE")
        try:
            with self.assertRaises(MigrationTimeoutError):
                prepare_database(
                    self.path, timeout=0.3, sleeper=lambda _s: None
                )
        finally:
            holder.execute("ROLLBACK")
            holder.close()

    # ------------------------------------------------------------------ #
    # 版本过新 / 结构被改坏 / 缺索引
    # ------------------------------------------------------------------ #

    def test_newer_schema_version_is_rejected(self) -> None:
        build_legacy_db(self.path, 3)
        with _connect(self.path) as conn:
            conn.execute(
                "CREATE TABLE schema_meta (id INTEGER PRIMARY KEY CHECK(id=1), "
                "schema_version INTEGER NOT NULL, upgraded_at TEXT NOT NULL, "
                "applied_count INTEGER NOT NULL DEFAULT 0)"
            )
            conn.execute(
                "INSERT INTO schema_meta(id, schema_version, upgraded_at) "
                "VALUES (1, 999, ?)", (_utcnow(),),
            )
            conn.commit()
        with self.assertRaises(SchemaVersionTooNewError) as ctx:
            prepare_database(self.path)
        self.assertEqual(ctx.exception.found, 999)

    def test_missing_required_index_blocks_readiness(self) -> None:
        build_legacy_db(self.path, 3, indexes=False)
        with self.assertRaises(SchemaDriftError) as ctx:
            prepare_database(self.path)
        problems = " ".join(ctx.exception.problems)
        self.assertIn("idx_impacts_root", problems)
        with self.assertRaises(SchemaDriftError):
            Repository(self.path)

    def test_unrecognizable_structure_is_rejected(self) -> None:
        conn = sqlite3.connect(str(self.path))
        conn.execute("CREATE TABLE events (x INTEGER)")  # 列完全对不上
        conn.execute("CREATE TABLE impacts (y INTEGER)")
        conn.commit()
        conn.close()
        with self.assertRaises(SchemaDriftError):
            prepare_database(self.path)

    def test_manually_degraded_current_schema_is_rejected(self) -> None:
        prepare_database(self.path)
        # 当前版本下手工删除必要索引（SQLite 可 DROP INDEX）。
        with _connect(self.path) as conn:
            conn.execute("DROP INDEX idx_events_airport")
            conn.commit()
        with self.assertRaises(SchemaDriftError) as ctx:
            prepare_database(self.path)
        self.assertTrue(
            any("idx_events_airport" in p for p in ctx.exception.problems)
        )

    def test_error_messages_never_contain_path(self) -> None:
        build_legacy_db(self.path, 3, indexes=False)
        with self.assertRaises(SchemaDriftError) as ctx:
            prepare_database(self.path)
        secret = str(self.path)
        self.assertNotIn(secret, str(ctx.exception))
        for problem in ctx.exception.problems:
            self.assertNotIn(secret, problem)
            self.assertNotIn(self._tmp.name, problem)


if __name__ == "__main__":
    unittest.main()
