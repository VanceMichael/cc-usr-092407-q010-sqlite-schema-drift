"""SQLite 持久化层。

单一数据库文件同时保存事件和计算结果，因此一次提交可以原子写入事件及其
全部影响，失败时也不会留下部分数据。数据库位于挂载卷时，WAL 模式可在
容器重启后继续保留数据。

结构（建表、列、索引与版本记录）完全由 :mod:`app.migrations` 管理；
``Repository`` 假定打开时数据库已通过 ``prepare_database`` 迁移并校验到
当前版本，因此这里不再执行任何 ``CREATE TABLE IF NOT EXISTS``——那条语句
正是早期“结构漂移却静默成功”问题的根源。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from app.migrations import light_verify_current_schema


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Repository:
    """对单一 SQLite 连接提供线程安全封装。

    打开时只设置连接参数并做一次结构复核；建表/迁移由
    ``migrations.prepare_database`` 在打开前完成。
    """

    def __init__(self, db_path: Path, *, verify: bool = True):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(db_path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute("PRAGMA synchronous=FULL")
            if verify:
                problems = light_verify_current_schema(self._conn)
                if problems:
                    self._conn.close()
                    from app.migrations import SchemaDriftError

                    raise SchemaDriftError(problems)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def ping(self) -> bool:
        with self._lock:
            row = self._conn.execute("SELECT 1").fetchone()
            return row is not None and row[0] == 1

    def ready_problems(self) -> list[str]:
        """就绪探针：返回当前结构问题；空列表表示可以接流量。"""
        with self._lock:
            problems = light_verify_current_schema(self._conn)
            if not problems and not self.ping():
                problems.append("storage is not responding")
            return problems

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
