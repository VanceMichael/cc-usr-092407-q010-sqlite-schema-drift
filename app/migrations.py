"""数据库 schema 版本记录、顺序迁移与结构校验。

设计要点
========

* ``schema_meta`` 表记录当前结构版本；服务每次启动都先读取并校验它。
* 所有待执行的迁移步骤在**同一个** ``BEGIN IMMEDIATE`` 事务中按顺序执行，
  版本号在最后写入。SQLite 的 DDL 是事务性的，进程崩溃或提交失败（例如磁盘
  不足）时整批回滚，磁盘上永远不会出现"执行了一半"的结构。
* 每个迁移步骤自身幂等（添加列前先检查列是否存在、建索引使用 IF NOT
  EXISTS），因此在任何中断之后重跑都是安全的。
* 多实例同时启动时，``BEGIN IMMEDIATE`` 的库级写锁充当选举锁：只有一个
  实例执行升级，其余实例在锁外等待，拿到锁后重新读取版本并复核结构。
* 版本比本进程新、结构被手工改坏、必要索引缺失或完整性检查失败时，结果为
  ``blocked``：调用方据此拒绝就绪，而不是继续对外提供服务。

诊断信息只包含版本号、列名、索引名等结构标识符，绝不包含数据库文件路径，
可以安全地通过 HTTP 诊断端点返回。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

SCHEMA_VERSION = 2
MIN_SUPPORTED_VERSION = 1

STATE_READY = "ready"
STATE_BLOCKED = "blocked"
STATE_LEGACY = "legacy"
STATE_EMPTY = "empty"

# 诊断原因代码（属于 HTTP 诊断契约，改动需保持兼容）。
REASON_UNSUPPORTED_VERSION = "unsupported_schema_version"
REASON_SCHEMA_DRIFT = "schema_drift"
REASON_MISSING_INDEX = "missing_index"
REASON_DATABASE_CORRUPT = "database_corrupt"
REASON_FOREIGN_KEYS = "referential_integrity_violation"
REASON_UNKNOWN_DATABASE = "unknown_database_state"
REASON_LOCK_TIMEOUT = "migration_lock_timeout"
REASON_MIGRATION_FAILED = "migration_failed"
REASON_STORAGE_UNAVAILABLE = "storage_unavailable"

# 崩溃注入：在第 N 个迁移步骤执行之后、提交之前以退出码 27 终止进程，
# 用于验证事务回滚不会留下半迁移状态。是否真的崩溃由调用方提供的
# crash_gate 决定（持久层用卷上的哨兵文件保证崩溃只发生一次，重启后
# 同一份环境变量不会造成崩溃循环）。仅用于测试与容器中断演练。
CRASH_ENV = "MIGRATION_CRASH_AFTER_STEP"
CRASH_EXIT_CODE = 27


def hard_exit_after_step(step_index: int) -> None:
    """默认崩溃动作：不执行任何 Python 清理直接退出，模拟硬崩溃/断电。"""

    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(CRASH_EXIT_CODE)

# --------------------------------------------------------------------------- #
# 当前结构（全新部署直接创建）
# --------------------------------------------------------------------------- #

CURRENT_SCHEMA = """
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
    replay_count         INTEGER NOT NULL DEFAULT 0,
    created_at           TEXT NOT NULL
);

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
);

CREATE INDEX idx_impacts_root    ON impacts(root_event_id);
CREATE INDEX idx_impacts_airport ON impacts(airport_code, impact_status);
CREATE INDEX idx_impacts_flight  ON impacts(flight_id);
CREATE INDEX idx_events_airport  ON events(airport_code, event_version);

CREATE TABLE schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

# 版本 1 的 events 表没有 replay_count / created_at，也没有
# idx_events_airport 索引；impacts 表结构与当前版本一致。
LEGACY_V1_SCHEMA = """
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
    payload_json         TEXT NOT NULL
);

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
);

CREATE INDEX idx_impacts_root    ON impacts(root_event_id);
CREATE INDEX idx_impacts_airport ON impacts(airport_code, impact_status);
CREATE INDEX idx_impacts_flight  ON impacts(flight_id);
"""

EVENTS_COLUMNS = {
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
    "replay_count",
    "created_at",
}
IMPACTS_COLUMNS = {
    "id",
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
}
REQUIRED_INDEXES = (
    "idx_impacts_root",
    "idx_impacts_airport",
    "idx_impacts_flight",
    "idx_events_airport",
)
REQUIRED_TABLES = ("events", "impacts", "schema_meta")

_INTERNAL_TABLES = {"sqlite_sequence"}
_PATH_LIKE = re.compile(r"/[\w./\-]+")


@dataclass(frozen=True)
class MigrationState:
    """一次启动引导的结论。可安全序列化给 HTTP 诊断端点（不含路径）。"""

    state: str
    current_version: int | None = None
    target_version: int = SCHEMA_VERSION
    role: str = "observed"
    applied_steps: tuple[str, ...] = field(default_factory=tuple)
    reason: str | None = None
    issues: tuple[str, ...] = field(default_factory=tuple)

    @property
    def ready(self) -> bool:
        return self.state == STATE_READY

    def public_dict(self) -> dict[str, Any]:
        body: dict[str, Any] = {
            "state": self.state,
            "current_version": self.current_version,
            "target_version": self.target_version,
            "role": self.role,
            "applied_steps": list(self.applied_steps),
        }
        if self.reason is not None:
            body["reason"] = self.reason
        if self.issues:
            body["issues"] = list(self.issues)
        return body


def _ready(
    version: int,
    role: str,
    applied_steps: list[str] | None = None,
) -> MigrationState:
    return MigrationState(
        state=STATE_READY,
        current_version=version,
        role=role,
        applied_steps=tuple(applied_steps or []),
    )


def _blocked(
    reason: str,
    issues: list[str] | None = None,
    *,
    current_version: int | None = None,
    role: str = "blocked",
) -> MigrationState:
    return MigrationState(
        state=STATE_BLOCKED,
        current_version=current_version,
        role=role,
        reason=reason,
        issues=tuple(issues or []),
    )


def _safe(text: str) -> str:
    """剔除错误文本中任何形似路径的内容，诊断接口不得泄露文件位置。"""

    return _PATH_LIKE.sub("<redacted>", text)


# --------------------------------------------------------------------------- #
# 迁移步骤定义（每个步骤必须幂等）
# --------------------------------------------------------------------------- #


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    return any(row[1] == column for row in conn.execute(f"PRAGMA table_info({table})"))


def _add_column_if_missing(
    conn: sqlite3.Connection, table: str, column: str, ddl: str
) -> bool:
    if _has_column(conn, table, column):
        return False
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")
    return True


def _write_meta_version(conn: sqlite3.Connection, version: int) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )
    applied_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn.executemany(
        "INSERT OR REPLACE INTO schema_meta (key, value) VALUES (?, ?)",
        (("schema_version", str(version)), ("applied_at", applied_at)),
    )


def _step_replay_count(conn: sqlite3.Connection) -> None:
    _add_column_if_missing(
        conn,
        "events",
        "replay_count",
        "replay_count INTEGER NOT NULL DEFAULT 0",
    )


def _step_created_at(conn: sqlite3.Connection) -> None:
    added = _add_column_if_missing(
        conn,
        "events",
        "created_at",
        "created_at TEXT NOT NULL DEFAULT ''",
    )
    if added or conn.execute(
        "SELECT 1 FROM events WHERE created_at = '' OR created_at IS NULL LIMIT 1"
    ).fetchone():
        # 旧行没有独立的写入时间戳：接收时间 reported_at 是最接近的可信值，
        # 且所有既有行都有该列。回填保持 latest-snapshot 排序的确定性。
        conn.execute(
            "UPDATE events SET created_at = reported_at "
            "WHERE created_at = '' OR created_at IS NULL"
        )


def _step_events_airport_index(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_events_airport "
        "ON events(airport_code, event_version)"
    )


def _step_schema_meta(conn: sqlite3.Connection) -> None:
    _write_meta_version(conn, SCHEMA_VERSION)


@dataclass(frozen=True)
class Migration:
    version: int
    description: str
    steps: tuple[tuple[str, Callable[[sqlite3.Connection], None]], ...]


# 按目标版本升序排列；新增结构变化时追加新的 Migration，禁止修改已发布步骤。
MIGRATIONS: tuple[Migration, ...] = (
    Migration(
        version=2,
        description="v1->v2: replay tracking columns, airport index, schema metadata",
        steps=(
            ("add events.replay_count", _step_replay_count),
            ("add events.created_at", _step_created_at),
            ("create idx_events_airport", _step_events_airport_index),
            ("record schema_meta version", _step_schema_meta),
        ),
    ),
)


# --------------------------------------------------------------------------- #
# 结构识别与校验
# --------------------------------------------------------------------------- #


def _user_tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()
    return {r[0] for r in rows} - _INTERNAL_TABLES


def _detect_version(conn: sqlite3.Connection) -> tuple[int | None, set[str]]:
    """返回 (版本号, 用户表集合)。

    有 schema_meta 时以记录的版本为准；没有 meta 但存在 events 表说明是
    早期服务留下的版本 1 数据库；没有任何用户表则是全新数据库；其余形态
    无法识别，返回 None 由调用方阻止就绪。
    """

    tables = _user_tables(conn)
    if "schema_meta" in tables:
        row = conn.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone()
        if row is None:
            return None, tables
        try:
            return int(row[0]), tables
        except (TypeError, ValueError):
            return None, tables
    if "events" in tables:
        return 1, tables
    if not tables:
        return 0, tables
    return None, tables


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def _index_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return {r[0] for r in rows}


def _integrity_issues(conn: sqlite3.Connection) -> tuple[str | None, list[str]]:
    quick = conn.execute("PRAGMA quick_check").fetchone()
    if quick is None or quick[0] != "ok":
        detail = quick[0] if quick else "no result"
        return REASON_DATABASE_CORRUPT, [f"quick_check: {detail}"]
    orphan_rows = conn.execute("PRAGMA foreign_key_check").fetchall()
    if orphan_rows:
        sample = ", ".join(f"{r[0]}#{r[1]}" for r in orphan_rows[:5])
        return REASON_FOREIGN_KEYS, [
            f"foreign_key_check: {len(orphan_rows)} violating row(s) ({sample})"
        ]
    return None, []


def _verify_target_schema(
    conn: sqlite3.Connection, *, include_integrity: bool = True
) -> list[tuple[str, str]]:
    """校验当前结构是否与 SCHEMA_VERSION 应有的形态一致。"""

    problems: list[tuple[str, str]] = []
    tables = _user_tables(conn)

    missing_tables = [t for t in REQUIRED_TABLES if t not in tables]
    if missing_tables:
        problems.extend(
            (REASON_SCHEMA_DRIFT, f"missing table: {name}") for name in missing_tables
        )
        return problems

    for table, required in (("events", EVENTS_COLUMNS), ("impacts", IMPACTS_COLUMNS)):
        for name in sorted(required - _table_columns(conn, table)):
            problems.append((REASON_SCHEMA_DRIFT, f"{table} missing column: {name}"))

    indexes = _index_names(conn)
    for name in REQUIRED_INDEXES:
        if name not in indexes:
            problems.append((REASON_MISSING_INDEX, f"missing index: {name}"))

    row = conn.execute(
        "SELECT value FROM schema_meta WHERE key = 'schema_version'"
    ).fetchone()
    if row is None:
        problems.append((REASON_SCHEMA_DRIFT, "schema_meta lacks schema_version"))
    else:
        try:
            recorded = int(row[0])
        except (TypeError, ValueError):
            problems.append(
                (REASON_SCHEMA_DRIFT,
                 f"schema_meta schema_version invalid: {row[0]!r}")
            )
        else:
            if recorded != SCHEMA_VERSION:
                problems.append(
                    (REASON_SCHEMA_DRIFT,
                     f"schema_meta records version {recorded}, expected {SCHEMA_VERSION}")
                )

    reason, issues = _integrity_issues(conn)
    if include_integrity and reason is not None:
        problems.extend((reason, item) for item in issues)
    return problems


def _verify_legacy_v1(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """对准备升级的版本 1 数据库做最低限度的形态与完整性检查。"""

    problems: list[tuple[str, str]] = []
    tables = _user_tables(conn)
    for name in ("events", "impacts"):
        if name not in tables:
            problems.append((REASON_SCHEMA_DRIFT, f"missing table: {name}"))
    if problems:
        return problems
    if "payload_json" not in _table_columns(conn, "events"):
        problems.append((REASON_SCHEMA_DRIFT, "events missing column: payload_json"))
    reason, issues = _integrity_issues(conn)
    if reason is not None:
        problems.extend((reason, item) for item in issues)
    return problems


def _group_problems(problems: list[tuple[str, str]]) -> tuple[str, list[str]]:
    """主原因按优先级选择，但 issues 保留全部问题，避免隐藏并发损坏。"""

    priority = (
        REASON_MISSING_INDEX,
        REASON_DATABASE_CORRUPT,
        REASON_FOREIGN_KEYS,
        REASON_SCHEMA_DRIFT,
    )
    rank = {reason: idx for idx, reason in enumerate(priority)}
    ordered = sorted(problems, key=lambda p: rank.get(p[0], len(priority)))
    return ordered[0][0], [issue for _, issue in ordered]


# --------------------------------------------------------------------------- #
# 引导（加锁选举 + 顺序迁移 + 复核）
# --------------------------------------------------------------------------- #


class _Blocked(Exception):
    """事务内判定不可继续：回滚并把状态返回给调用方。"""

    def __init__(self, state: MigrationState):
        super().__init__(state.reason)
        self.state = state


def _acquire_immediate_once(conn: sqlite3.Connection) -> None:
    """尝试一次 BEGIN IMMEDIATE；拿不到锁时抛出 OperationalError。"""

    conn.execute("PRAGMA busy_timeout=0")
    conn.execute("BEGIN IMMEDIATE")


def _is_lock_error(exc: sqlite3.OperationalError) -> bool:
    text = str(exc).lower()
    return "locked" in text or "busy" in text


def _peek_while_waiting(conn: sqlite3.Connection) -> MigrationState | None:
    """等待写锁期间做一次只读复核。

    领导者已提交并把结构带到目标版本时，跟随者无需再拿锁，直接以
    follower 身份复核通过；发现确定性阻断（版本过新、结构漂移、损坏）时
    立即返回 blocked；迁移看起来仍在进行则返回 None 继续等待。
    """

    try:
        version, tables = _detect_version(conn)
        if version == SCHEMA_VERSION:
            # 等待期做轻量结构复核即可；拿到锁的实例会做完整性检查。
            problems = _verify_target_schema(conn, include_integrity=False)
            if not problems:
                return _ready(SCHEMA_VERSION, "follower")
            reason, issues = _group_problems(problems)
            return _blocked(reason, issues, current_version=SCHEMA_VERSION, role="follower")
        if version is not None and version > SCHEMA_VERSION:
            return _blocked(
                REASON_UNSUPPORTED_VERSION,
                [f"database schema version {version} is newer than this binary "
                 f"supports (highest: {SCHEMA_VERSION})"],
                current_version=version,
                role="follower",
            )
        if version is None:
            return _blocked(
                REASON_UNKNOWN_DATABASE,
                [f"unrecognised database contents; tables present: {sorted(tables)}"],
                role="follower",
            )
        # version == 0（尚未初始化）或旧版本：领导者仍在迁移，继续等待。
        return None
    except sqlite3.Error:
        # 读失败可能只是与检查点瞬时冲突；继续等待，由超时兜底。
        return None


def _exec_script_in_transaction(conn: sqlite3.Connection, script: str) -> None:
    """在当前事务内逐条执行 DDL（executescript 会隐式提交，不能用）。"""

    statement = []
    for line in script.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        statement.append(line)
        if stripped.endswith(";"):
            conn.execute("\n".join(statement))
            statement = []
    if statement:
        raise ValueError("schema script has a statement without terminating semicolon")


def bootstrap_database(
    conn: sqlite3.Connection,
    *,
    lock_timeout: float = 30.0,
    poll_interval: float = 0.1,
    crash_after_step: int | None = None,
    on_before_commit: Callable[[], None] | None = None,
    crash_action: Callable[[int], None] = hard_exit_after_step,
) -> MigrationState:
    """在已打开的连接上完成"选举 → 识别 → 迁移 → 校验"全过程。

    ``BEGIN IMMEDIATE`` 是选举锁：一次拿到锁的实例是领导者；锁被占用时
    其余实例周期性只读复核，领导者一旦提交完成，跟随者立即以复核结论
    就绪；看到确定性阻断则同步阻断；超过 ``lock_timeout`` 仍未拿到锁也
    未见完成，才返回 migration_lock_timeout。

    ``crash_after_step`` 给定时，应用到该序号的迁移步骤后、提交前调用
    ``crash_action``（默认硬退出）。调用方负责一次性闸门，避免重启循环。
    """

    deadline = time.monotonic() + max(0.0, lock_timeout)
    waited = False
    while True:
        try:
            _acquire_immediate_once(conn)
            break
        except sqlite3.OperationalError as exc:
            if not _is_lock_error(exc):
                return _blocked(
                    REASON_STORAGE_UNAVAILABLE,
                    [f"cannot acquire write transaction: {_safe(str(exc))}"],
                )
            waited = True
            peeked = _peek_while_waiting(conn)
            if peeked is not None:
                return peeked
            if time.monotonic() >= deadline:
                return _blocked(
                    REASON_LOCK_TIMEOUT,
                    [f"timed out after {lock_timeout:.0f}s waiting for the migration lock"],
                    role="follower",
                )
            time.sleep(poll_interval)
        except sqlite3.Error as exc:
            return _blocked(
                REASON_STORAGE_UNAVAILABLE,
                [f"cannot acquire write transaction: {_safe(str(exc))}"],
            )

    try:
        state = _bootstrap_locked(
            conn,
            waited=waited,
            crash_after_step=crash_after_step,
            on_before_commit=on_before_commit,
            crash_action=crash_action,
        )
    except _Blocked as blocked:
        state = blocked.state
    except sqlite3.Error as exc:
        state = _blocked(
            REASON_MIGRATION_FAILED,
            [f"{type(exc).__name__}: {_safe(str(exc))}"],
        )
    except Exception as exc:  # 防御：任何意外都不允许带着半事务返回
        state = _blocked(REASON_MIGRATION_FAILED, [_safe(str(exc) or type(exc).__name__)])

    if conn.in_transaction:
        if state.ready:
            # 正常路径在内部已提交；此时提交属于防御性收尾。
            conn.execute("COMMIT")
        else:
            conn.execute("ROLLBACK")
    return state


def _bootstrap_locked(
    conn: sqlite3.Connection,
    *,
    waited: bool,
    crash_after_step: int | None,
    on_before_commit: Callable[[], None] | None,
    crash_action: Callable[[int], None],
) -> MigrationState:
    version, tables = _detect_version(conn)

    # 全新数据库：在本事务内直接创建当前版本结构。
    if version == 0:
        if tables:
            raise _Blocked(
                _blocked(
                    REASON_UNKNOWN_DATABASE,
                    [f"unexpected user tables before initialisation: {sorted(tables)}"],
                )
            )
        _exec_script_in_transaction(conn, CURRENT_SCHEMA)
        _write_meta_version(conn, SCHEMA_VERSION)
        if on_before_commit is not None:
            on_before_commit()
        conn.execute("COMMIT")
        return _ready(SCHEMA_VERSION, "initialized")

    if version is None:
        raise _Blocked(
            _blocked(
                REASON_UNKNOWN_DATABASE,
                ["database is neither empty, a supported legacy schema, nor a "
                 f"versioned schema; tables present: {sorted(tables)}"],
            )
        )

    if version > SCHEMA_VERSION:
        raise _Blocked(
            _blocked(
                REASON_UNSUPPORTED_VERSION,
                [f"database schema version {version} is newer than this binary "
                 f"supports (highest: {SCHEMA_VERSION})"],
                current_version=version,
                role="follower" if waited else "blocked",
            )
        )

    if version < MIN_SUPPORTED_VERSION:
        raise _Blocked(
            _blocked(
                REASON_UNSUPPORTED_VERSION,
                [f"database schema version {version} is older than the oldest "
                 f"supported version {MIN_SUPPORTED_VERSION}"],
                current_version=version,
            )
        )

    # 旧版本：先体检，再在同一事务内按顺序应用所有待执行迁移。
    if version < SCHEMA_VERSION:
        legacy_problems = _verify_legacy_v1(conn) if version == 1 else []
        if legacy_problems:
            reason, issues = _group_problems(legacy_problems)
            raise _Blocked(_blocked(reason, issues, current_version=version))

        pending = tuple(m for m in MIGRATIONS if m.version > version)
        expected_next = version + 1
        applied_labels: list[str] = []
        for migration in pending:
            if migration.version != expected_next:
                raise _Blocked(
                    _blocked(
                        REASON_MIGRATION_FAILED,
                        [f"no migration path from version {expected_next - 1} "
                         f"to {migration.version}"],
                        current_version=version,
                    )
                )
            for label, step in migration.steps:
                step(conn)
                applied_labels.append(label)
                if crash_after_step is not None and crash_after_step == len(applied_labels):
                    # 硬崩溃：事务未提交，下次打开时 SQLite 自动回滚。
                    crash_action(len(applied_labels))
            expected_next = migration.version + 1

        # 提交前在同一事务内复核迁移结果；不达标则整体回滚，绝不发布半结构。
        post_problems = _verify_target_schema(conn)
        if post_problems:
            reason, issues = _group_problems(post_problems)
            raise _Blocked(_blocked(reason, issues, current_version=SCHEMA_VERSION))

        if on_before_commit is not None:
            on_before_commit()
        conn.execute("COMMIT")
        return _ready(SCHEMA_VERSION, "leader", applied_labels)

    # 版本已是最新：复核结构是否被手工改坏、索引是否齐全。
    problems = _verify_target_schema(conn)
    if problems:
        reason, issues = _group_problems(problems)
        raise _Blocked(
            _blocked(reason, issues, current_version=SCHEMA_VERSION,
                     role="follower" if waited else "blocked")
        )
    conn.execute("COMMIT")
    return _ready(SCHEMA_VERSION, "follower" if waited else "verified")


def observe_database(conn: sqlite3.Connection) -> MigrationState:
    """只读观察当前数据库状态（不获取写锁、不执行任何变更）。"""

    version, tables = _detect_version(conn)
    if version is None:
        return _blocked(
            REASON_UNKNOWN_DATABASE,
            [f"unrecognised database contents; tables present: {sorted(tables)}"],
        )
    if version == 0:
        return MigrationState(state=STATE_EMPTY, current_version=0, role="observed")
    if version > SCHEMA_VERSION:
        return _blocked(
            REASON_UNSUPPORTED_VERSION,
            [f"database schema version {version} is newer than this binary supports "
             f"(highest: {SCHEMA_VERSION})"],
            current_version=version,
        )
    if version < SCHEMA_VERSION:
        problems = _verify_legacy_v1(conn) if version == 1 else []
        if problems:
            reason, issues = _group_problems(problems)
            return _blocked(reason, issues, current_version=version)
        return MigrationState(
            state=STATE_LEGACY, current_version=version, role="observed"
        )
    problems = _verify_target_schema(conn)
    if problems:
        reason, issues = _group_problems(problems)
        return _blocked(reason, issues, current_version=SCHEMA_VERSION)
    return _ready(SCHEMA_VERSION, "observed")


# --------------------------------------------------------------------------- #
# 命令行入口（供多进程/中断演练与正式测试使用）
# --------------------------------------------------------------------------- #


def _open_connection(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=FULL")
    return conn


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Schema migration utility")
    sub = parser.add_subparsers(dest="command", required=True)

    p_up = sub.add_parser("upgrade", help="migrate a database to the current version")
    p_up.add_argument("db_path")
    p_up.add_argument("--lock-timeout", type=float, default=30.0)
    p_up.add_argument("--hold-before-commit", type=float, default=0.0)
    p_up.add_argument("--crash-after-step", type=int, default=None)

    p_in = sub.add_parser("inspect", help="observe database state without modifying it")
    p_in.add_argument("db_path")

    args = parser.parse_args(argv)

    if args.command == "inspect":
        from urllib.request import pathname2url

        url = "file:" + pathname2url(os.path.abspath(args.db_path)) + "?mode=ro"
        try:
            conn = sqlite3.connect(url, uri=True, isolation_level=None)
            conn.row_factory = sqlite3.Row
            state = observe_database(conn)
        except sqlite3.Error as exc:
            state = _blocked(
                REASON_STORAGE_UNAVAILABLE,
                [f"{type(exc).__name__}: {_safe(str(exc))}"],
            )
        print(json.dumps(state.public_dict(), ensure_ascii=False, indent=2, sort_keys=True))
        return 0 if state.state != STATE_BLOCKED else 3

    def hold() -> None:
        if args.hold_before_commit:
            time.sleep(args.hold_before_commit)

    crash_after_step = args.crash_after_step
    if crash_after_step is None and os.environ.get(CRASH_ENV):
        try:
            crash_after_step = int(os.environ[CRASH_ENV])
        except ValueError:
            crash_after_step = None

    try:
        conn = _open_connection(args.db_path)
    except sqlite3.Error as exc:
        state = _blocked(
            REASON_STORAGE_UNAVAILABLE,
            [f"{type(exc).__name__}: {_safe(str(exc))}"],
        )
        print(json.dumps(state.public_dict(), ensure_ascii=False, indent=2, sort_keys=True))
        return 4
    state = bootstrap_database(
        conn,
        lock_timeout=args.lock_timeout,
        crash_after_step=crash_after_step,
        on_before_commit=hold,
    )
    print(json.dumps(state.public_dict(), ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if state.ready else 3


if __name__ == "__main__":
    sys.exit(_main())
