"""SQLite 持久化层。

单一数据库文件同时保存事件和计算结果，因此一次提交可以原子写入事件及其
全部影响，失败时也不会留下部分数据。数据库位于挂载卷时，WAL 模式可在
容器重启后继续保留数据。

结构不再由 ``CREATE TABLE IF NOT EXISTS`` 隐式创建：打开数据库时先由
``app.migrations`` 读取并校验 schema 版本、在选举锁保护下完成顺序迁移，
只有结论为 ready 时仓储才允许对外提供读写（由服务层/HTTP 层据此拦截）。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from app.migrations import (
    CRASH_ENV,
    REASON_LOCK_TIMEOUT,
    REASON_MIGRATION_FAILED,
    REASON_STORAGE_UNAVAILABLE,
    MigrationState,
    bootstrap_database,
    hard_exit_after_step,
)


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# 这些结论可能在下一轮引导中改变（锁被释放、磁盘恢复、领导者完成迁移），
# 其余结论（版本过新、结构漂移、索引缺失、损坏）是确定性的，重试无意义。
_TRANSIENT_REASONS = frozenset(
    {
        REASON_LOCK_TIMEOUT,
        REASON_STORAGE_UNAVAILABLE,
        REASON_MIGRATION_FAILED,
    }
)


def _crash_gate(db_path: Path) -> tuple[int | None, Any]:
    """解析一次性崩溃注入开关。

    MIGRATION_CRASH_AFTER_STEP=N 时，仅当同目录哨兵 ``.migration-crash-sent``
    不存在才在第 N 步后硬崩溃，并在崩溃前落下哨兵。这样容器带着相同环境
    变量重启时第二次启动会正常完成迁移，而不是陷入崩溃循环。
    """

    raw = os.environ.get(CRASH_ENV)
    if not raw:
        return None, None
    try:
        step = int(raw)
    except ValueError:
        return None, None
    sentinel = db_path.parent / ".migration-crash-sent"
    if sentinel.exists():
        return None, None

    def gated_crash(applied: int) -> None:
        # 构造期到真正执行到该步骤之间，可能已有其他实例先崩溃并落了哨兵
        # （它持锁先跑）。以哨兵的最终存在性为准，保证整卷只崩溃一次，
        # 接管迁移的跟随者会正常完成。
        if sentinel.exists():
            return
        # 先持久化哨兵（与数据库在同一卷），再硬退出。
        sentinel.write_text(f"crashed_after_step={applied}\n", encoding="utf-8")
        hard_exit_after_step(applied)

    return step, gated_crash


class Repository:
    """对单一 SQLite 连接提供线程安全封装。"""

    def __init__(self, db_path: Path, *, migration_lock_timeout: float = 30.0):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(db_path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            # journal_mode=WAL 是持久化设置（之后的打开只读该值）；给一个
            # 短暂的 busy_timeout，避免多实例首次打开时在 WAL 切换上立刻
            # 撞上 SQLITE_BUSY。
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute("PRAGMA synchronous=FULL")
            # 选举与迁移（内部会把 busy_timeout 临时置 0 自行轮询）。
            crash_step, crash_action = _crash_gate(db_path)
            kwargs: dict[str, Any] = {"lock_timeout": migration_lock_timeout}
            if crash_step is not None:
                kwargs["crash_after_step"] = crash_step
                kwargs["crash_action"] = crash_action
            self._migration_state: MigrationState = bootstrap_database(
                self._conn, **kwargs
            )
            # 正常请求期允许短暂等待其他进程释放写锁。
            self._conn.execute("PRAGMA busy_timeout=5000")

    @property
    def migration_state(self) -> MigrationState:
        return self._migration_state

    @property
    def ready(self) -> bool:
        if self._migration_state.ready:
            return True
        # 瞬时阻断（等锁超时、存储暂时不可用、迁移步骤失败已整体回滚）：
        # 在就绪探针上重新走一遍"选举 → 复核"，让跟随者在领导者完成后
        # 无需重启即可就绪。探针间隔本身就是重试节流。确定性阻断（版本
        # 过新、结构漂移、索引缺失、损坏）重试不会改变结论，保持 blocked。
        if self._migration_state.reason in _TRANSIENT_REASONS:
            with self._lock:
                try:
                    # 预算必须短于探针超时：领导者已提交时内部的只读复核
                    # 会立即返回，只有在领导者长时间持锁时才会等满。
                    self._migration_state = bootstrap_database(
                        self._conn, lock_timeout=2.0
                    )
                finally:
                    # bootstrap 会把 busy_timeout 置 0；恢复请求期等待预算。
                    self._conn.execute("PRAGMA busy_timeout=5000")
        return self._migration_state.ready

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def ping(self) -> bool:
        with self._lock:
            row = self._conn.execute("SELECT 1").fetchone()
            return row is not None and row[0] == 1

    # ------------------------------------------------------------------ #
    # Reads
    # ------------------------------------------------------------------ #

    def get_event_row(self, event_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()

    def get_impacts(self, event_id: str) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM impacts WHERE event_id = ? ORDER BY flight_id",
                    (event_id,),
                )
            )

    def events_for_airport(self, airport_code: str) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM events WHERE airport_code = ? "
                    "ORDER BY event_version, effective_from",
                    (airport_code,),
                )
            )

    def count_replays(self, event_id: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT replay_count FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()
            return int(row["replay_count"]) if row else 0

    def latest_impacts(
        self,
        *,
        airport: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        """返回每个航班与机场组合的最新影响。

        同一机场内，每条事件链采用最新事件的快照；航班同时出现在多条链时，
        采用最后生成的快照。`resolved` 墓碑参与排序，使恢复开放后释放的航班
        不再出现在结果中。最终结果按 flight_id 稳定排序。
        """
        where = ["i.airport_code = ?"] if airport else []
        params: list[Any] = [airport] if airport else []
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        outer = ["rn = 1", "impact_status != 'resolved'"]
        outer_params: list[Any] = []
        if status:
            outer.append("impact_status = ?")
            outer_params.append(status)

        sql = f"""
        WITH ranked AS (
            SELECT i.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY i.flight_id, i.airport_code
                       ORDER BY e.created_at DESC,
                                i.id DESC
                   ) AS rn
            FROM impacts i
            JOIN events e ON e.event_id = i.event_id
            {where_sql}
        )
        SELECT * FROM ranked
        WHERE {' AND '.join(outer)}
        ORDER BY flight_id, airport_code
        """
        with self._lock:
            return [dict(r) for r in self._conn.execute(sql, params + outer_params)]

    def prior_chain_impact_ids(self, conn, root_event_id: str) -> set[str]:
        rows = conn.execute(
            "SELECT DISTINCT flight_id FROM impacts WHERE root_event_id = ? "
            "AND impact_status != 'resolved'",
            (root_event_id,),
        ).fetchall()
        return {r["flight_id"] for r in rows}

    # ------------------------------------------------------------------ #
    # Writes (all callers run inside ``transaction``)
    # ------------------------------------------------------------------ #

    def transaction(self):
        return _Transaction(self._conn, self._lock)

    def insert_event(self, conn: sqlite3.Connection, event_dict: dict[str, Any]) -> None:
        conn.execute(
            """
            INSERT INTO events (event_id, event_version, event_type, airport_code,
                                effective_from, effective_until, reported_at,
                                supersedes_event_id, reason, payload_json,
                                replay_count, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
            """,
            (
                event_dict["event_id"],
                event_dict["event_version"],
                event_dict["event_type"],
                event_dict["airport_code"],
                event_dict["effective_from"],
                event_dict["effective_until"],
                event_dict["reported_at"],
                event_dict["supersedes_event_id"],
                event_dict["reason"],
                json.dumps(event_dict, sort_keys=True, ensure_ascii=False),
                utcnow_iso(),
            ),
        )

    def insert_impacts(
        self, conn: sqlite3.Connection, impacts: Iterable[dict[str, Any]]
    ) -> None:
        conn.executemany(
            """
            INSERT INTO impacts (event_id, root_event_id, airport_code, flight_id,
                                 flight_number, affected_endpoint, impact_status,
                                 overlap_minutes, delay_minutes, proposed_departure,
                                 proposed_arrival, passenger_count, crosses_midnight)
            VALUES (:event_id, :root_event_id, :airport_code, :flight_id,
                    :flight_number, :affected_endpoint, :impact_status,
                    :overlap_minutes, :delay_minutes, :proposed_departure,
                    :proposed_arrival, :passenger_count, :crosses_midnight)
            """,
            list(impacts),
        )

    def increment_replay(self, conn: sqlite3.Connection, event_id: str) -> None:
        conn.execute(
            "UPDATE events SET replay_count = replay_count + 1 WHERE event_id = ?",
            (event_id,),
        )


class _Transaction:
    """管理 BEGIN IMMEDIATE、COMMIT 与 ROLLBACK 的事务上下文。"""

    def __init__(self, conn: sqlite3.Connection, lock: threading.RLock):
        self._conn = conn
        self._lock = lock

    def __enter__(self) -> sqlite3.Connection:
        self._lock.acquire()
        self._conn.execute("BEGIN IMMEDIATE")
        return self._conn

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if exc_type is None:
                self._conn.execute("COMMIT")
            else:
                self._conn.execute("ROLLBACK")
        finally:
            self._lock.release()
