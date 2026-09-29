"""数据库 schema 版本记录、校验与顺序迁移。

设计要点
--------

* ``schema_meta`` 记录当前结构版本，``schema_migrations`` 记录每个版本是
  基线建立还是迁移得到；每次启动都必须能读到与二进制期望一致的版本。
* 升级按版本号顺序执行，整段升级包在 *一个* ``BEGIN IMMEDIATE`` 事务里：
  任一步失败或进程崩溃，SQLite 都会回滚整次迁移，磁盘上不会留下
  “升了一半”的结构。每个步骤本身幂等，崩溃后由新的执行者整体重跑。
* ``BEGIN IMMEDIATE`` 立刻获取文件级保留锁：多实例同时启动时只有一个
  实例能成为执行者，其余实例轮询版本表，待其提交后复核结构再开放流量。
* 版本比二进制更新、结构被手工改坏（缺列/缺索引/完整性损坏）都会抛出
  明确异常，由调用方把进程保持在未就绪状态，而不是带病接流量。

所有面向外部的错误信息只描述结构问题，绝不包含文件系统路径。
"""

from __future__ import annotations

import hashlib
import random
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

CURRENT_SCHEMA_VERSION = 3

# --------------------------------------------------------------------------- #
# 结构定义
# --------------------------------------------------------------------------- #

EVENTS_COLUMNS_V3 = (
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
)

# v1 的 events 没有 replay_count；v2 起新增。
EVENTS_COLUMNS_V1 = tuple(c for c in EVENTS_COLUMNS_V3 if c != "replay_count")

IMPACTS_COLUMNS_V3 = (
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
)

# v1/v2 的 impacts 没有 passenger_count、crosses_midnight。
IMPACTS_COLUMNS_V1 = tuple(
    c for c in IMPACTS_COLUMNS_V3
    if c not in ("passenger_count", "crosses_midnight")
)

REQUIRED_INDEXES_V3 = (
    "idx_impacts_root",
    "idx_impacts_airport",
    "idx_impacts_flight",
    "idx_events_airport",
)

META_TABLE_SQL = (
    """
CREATE TABLE IF NOT EXISTS schema_meta (
    id             INTEGER PRIMARY KEY CHECK (id = 1),
    schema_version INTEGER NOT NULL,
    upgraded_at    TEXT NOT NULL,
    applied_count  INTEGER NOT NULL DEFAULT 0
)
""",
    """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INTEGER PRIMARY KEY,
    action     TEXT NOT NULL,
    applied_at TEXT NOT NULL,
    checksum   TEXT
)
""",
)

# 全新数据库直接建立 v3 基线。
_BASELINE_TABLES = (
    """
CREATE TABLE IF NOT EXISTS events (
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
)
""",
    """
CREATE TABLE IF NOT EXISTS impacts (
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
""",
)

_BASELINE_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_impacts_root    ON impacts(root_event_id)",
    "CREATE INDEX IF NOT EXISTS idx_impacts_airport ON impacts(airport_code, impact_status)",
    "CREATE INDEX IF NOT EXISTS idx_impacts_flight  ON impacts(flight_id)",
    "CREATE INDEX IF NOT EXISTS idx_events_airport  ON events(airport_code, event_version)",
)

# 每个升级步骤保留规范化文本，写入 schema_migrations.checksum，之后打开时
# 复核历史记录没有被替换。
_STEP_2_SQL = (
    "ALTER TABLE events ADD COLUMN replay_count INTEGER NOT NULL DEFAULT 0"
)

_STEP_3_SQL = """
CREATE TABLE impacts_new (
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
    passenger_count    INTEGER NOT NULL DEFAULT 0,
    crosses_midnight   INTEGER NOT NULL DEFAULT 0,
    UNIQUE(event_id, flight_id, airport_code)
)
""".strip()


def _checksum(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


STEP_CHECKSUMS = {
    2: _checksum(_STEP_2_SQL),
    3: _checksum(_STEP_3_SQL),
}


# --------------------------------------------------------------------------- #
# 异常（对外信息不含路径）
# --------------------------------------------------------------------------- #


class MigrationError(Exception):
    """迁移/校验失败的基类，``code`` 会出现在就绪与诊断响应中。"""

    code = "migration_failed"

    def __init__(self, message: str, *, problems: list[str] | None = None):
        super().__init__(message)
        self.message = message
        self.problems = problems or []


class SchemaVersionTooNewError(MigrationError):
    code = "schema_version_too_new"

    def __init__(self, found: int, supported: int):
        super().__init__(
            f"database schema version {found} is newer than this binary supports "
            f"(supported up to {supported})"
        )
        self.found = found
        self.supported = supported


class SchemaDriftError(MigrationError):
    code = "schema_drift"

    def __init__(self, problems: list[str]):
        super().__init__(
            "database structure does not match the recorded schema version",
            problems=problems,
        )


class MigrationTimeoutError(MigrationError):
    code = "migration_timeout"

    def __init__(self) -> None:
        super().__init__("timed out waiting for another instance to finish schema migration")


# --------------------------------------------------------------------------- #
# 结果
# --------------------------------------------------------------------------- #


@dataclass
class PrepareResult:
    schema_version: int
    from_version: int
    actions: list[str] = field(default_factory=list)
    migrated_steps: list[int] = field(default_factory=list)
    elapsed_ms: int = 0


# --------------------------------------------------------------------------- #
# 连接与状态读取
# --------------------------------------------------------------------------- #


def connect_admin(db_path: Path) -> sqlite3.Connection:
    """打开用于迁移的裸连接：WAL、外键开启、不自动提交。"""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), isolation_level=None, timeout=0)
    conn.row_factory = sqlite3.Row
    # 锁等待由 prepare_database 的轮询循环负责；同时保留一个较短的
    # busy_timeout，避免瞬时锁竞争直接报错。
    conn.execute("PRAGMA busy_timeout=250")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=FULL")
    return conn


def _table_names(conn: sqlite3.Connection) -> set[str]:
    return {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}


def _index_names(conn: sqlite3.Connection) -> set[str]:
    return {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND name NOT LIKE 'sqlite_%'"
        )
    }


@dataclass
class _DbState:
    meta_version: int | None  # schema_meta 中记录的版本；表不存在或为空时另行区分
    meta_table_present: bool
    legacy_version: int | None  # 0=全新, 1/2/3=无版本表的旧版, None=无法识别


def _read_state(conn: sqlite3.Connection) -> _DbState:
    tables = _table_names(conn)

    meta_version: int | None = None
    meta_present = "schema_meta" in tables
    if meta_present:
        row = conn.execute(
            "SELECT schema_version FROM schema_meta WHERE id = 1"
        ).fetchone()
        if row is None:
            # 版本表存在但没有版本行，属于损坏/被手工篡改。
            return _DbState(None, True, None)
        meta_version = int(row["schema_version"])

    if "events" not in tables:
        # 没有业务表时，只允许“真正的空库"被初始化；任何孤立的业务表或
        # 半截版本记录都视为漂移，避免覆盖别人的数据。
        allowed_empty = {"schema_meta", "schema_migrations"} if meta_present else set()
        leftover = tables - allowed_empty
        legacy = 0 if not leftover else None
        return _DbState(meta_version, meta_present, legacy)

    if "impacts" not in tables:
        return _DbState(meta_version, meta_present, None)

    # 版本历史表存在、版本表却缺失：典型的手工删除，拒绝猜测。
    if not meta_present and "schema_migrations" in tables:
        return _DbState(meta_version, meta_present, None)

    event_cols = _columns(conn, "events")
    impact_cols = _columns(conn, "impacts")

    v3_events = set(EVENTS_COLUMNS_V3)
    v1_events = set(EVENTS_COLUMNS_V1)
    v3_impacts = set(IMPACTS_COLUMNS_V3)
    v1_impacts = set(IMPACTS_COLUMNS_V1)

    # 精确匹配列集合：任何手工增删列都会落到 None（漂移），而不是被当成
    # 某个受支持旧版本“凑合升级”，从而避免迁移在未知结构上丢数据。
    if event_cols == v3_events and impact_cols == v3_impacts:
        legacy = 3
    elif event_cols == v3_events and impact_cols == v1_impacts:
        legacy = 2
    elif event_cols == v1_events and impact_cols == v1_impacts:
        legacy = 1
    else:
        legacy = None
    return _DbState(meta_version, meta_present, legacy)


# --------------------------------------------------------------------------- #
# 升级步骤（每个都必须幂等）
# --------------------------------------------------------------------------- #


def _migrate_1_to_2(conn: sqlite3.Connection) -> None:
    # v2：事件增加重放计数，已有行回填为 0。
    if "replay_count" not in _columns(conn, "events"):
        conn.execute(_STEP_2_SQL)


def _migrate_2_to_3(conn: sqlite3.Connection) -> None:
    # v3：impacts 增加 passenger_count 与 crosses_midnight。SQLite 不能
    # ALTER ADD 一个没有常量默认值表达式以外的 NOT NULL 复杂列，这里采用
    # 官方推荐的“建新表-拷贝-改名”流程，全部在同一事务内完成。
    impact_cols = _columns(conn, "impacts")
    if {"passenger_count", "crosses_midnight"} <= impact_cols:
        return
    conn.execute(_STEP_3_SQL)
    conn.execute(
        """
    INSERT INTO impacts_new (
        id, event_id, root_event_id, airport_code, flight_id, flight_number,
        affected_endpoint, impact_status, overlap_minutes, delay_minutes,
        proposed_departure, proposed_arrival, passenger_count, crosses_midnight
    )
    SELECT
        id, event_id, root_event_id, airport_code, flight_id, flight_number,
        affected_endpoint, impact_status, overlap_minutes, delay_minutes,
        proposed_departure, proposed_arrival, 0, 0
    FROM impacts
    """
    )
    conn.execute("DROP TABLE impacts")
    conn.execute("ALTER TABLE impacts_new RENAME TO impacts")
    for stmt in _BASELINE_INDEXES:
        conn.execute(stmt)


MIGRATIONS: dict[int, tuple[str, Callable[[sqlite3.Connection], None]]] = {
    2: (_STEP_2_SQL, _migrate_1_to_2),
    3: (_STEP_3_SQL, _migrate_2_to_3),
}


# --------------------------------------------------------------------------- #
# 结构校验
# --------------------------------------------------------------------------- #


def _verify_current_schema(conn: sqlite3.Connection) -> list[str]:
    """返回结构问题列表；空列表代表与 v3 完全一致。只报告，不含路径。"""
    problems: list[str] = []
    tables = _table_names(conn)

    for required in ("events", "impacts", "schema_meta", "schema_migrations"):
        if required not in tables:
            problems.append(f"required table is missing: {required}")
    if problems:
        return problems

    expected = {
        "events": set(EVENTS_COLUMNS_V3),
        "impacts": set(IMPACTS_COLUMNS_V3),
    }
    for table, want in expected.items():
        have = _columns(conn, table)
        for missing in sorted(want - have):
            problems.append(f"{table} table is missing column: {missing}")
        for extra in sorted(have - want):
            problems.append(f"{table} table has unexpected column: {extra}")

    indexes = _index_names(conn)
    for missing in REQUIRED_INDEXES_V3:
        if missing not in indexes:
            problems.append(f"required index is missing: {missing}")

    row = conn.execute(
        "SELECT schema_version FROM schema_meta WHERE id = 1"
    ).fetchone()
    if row is None:
        problems.append("schema_meta has no version row")
    elif int(row["schema_version"]) != CURRENT_SCHEMA_VERSION:
        problems.append(
            f"schema_meta records version {int(row['schema_version'])}, "
            f"expected {CURRENT_SCHEMA_VERSION}"
        )

    recorded = {
        int(r["version"]): (r["action"], r["checksum"])
        for r in conn.execute("SELECT version, action, checksum FROM schema_migrations")
    }
    if {int(v) for v in recorded} != set(range(1, CURRENT_SCHEMA_VERSION + 1)):
        problems.append("schema_migrations history does not cover versions 1..3")
    for version, (action, checksum) in recorded.items():
        if action == "migrate":
            expected_checksum = STEP_CHECKSUMS.get(version)
            if expected_checksum is not None and checksum != expected_checksum:
                problems.append(f"migration {version} checksum does not match")
        elif action not in ("baseline",):
            problems.append(f"migration {version} has unknown action: {action}")

    integrity = conn.execute("PRAGMA integrity_check").fetchone()
    if integrity is None or integrity[0] != "ok":
        problems.append(f"database integrity check failed: {integrity[0] if integrity else 'no result'}")

    fk_violations = conn.execute("PRAGMA foreign_key_check").fetchmany(2)
    if fk_violations:
        problems.append("database has foreign key violations")

    return problems


def light_verify_current_schema(conn: sqlite3.Connection) -> list[str]:
    """就绪探针使用的快速结构检查：版本、必备表/列/索引是否在位。

    与 :func:`_verify_current_schema` 不同，这里不做 ``integrity_check`` 与
    ``foreign_key_check``（全库扫描），适合每次探针调用。
    """
    problems: list[str] = []
    tables = _table_names(conn)
    for required in ("events", "impacts", "schema_meta", "schema_migrations"):
        if required not in tables:
            problems.append(f"required table is missing: {required}")
    if problems:
        return problems

    row = conn.execute(
        "SELECT schema_version FROM schema_meta WHERE id = 1"
    ).fetchone()
    if row is None:
        problems.append("schema_meta has no version row")
    elif int(row["schema_version"]) != CURRENT_SCHEMA_VERSION:
        problems.append(
            f"schema_meta records version {int(row['schema_version'])}, "
            f"expected {CURRENT_SCHEMA_VERSION}"
        )

    for table, want in (
        ("events", set(EVENTS_COLUMNS_V3)),
        ("impacts", set(IMPACTS_COLUMNS_V3)),
    ):
        missing = want - _columns(conn, table)
        for name in sorted(missing):
            problems.append(f"{table} table is missing column: {name}")

    indexes = _index_names(conn)
    for name in REQUIRED_INDEXES_V3:
        if name not in indexes:
            problems.append(f"required index is missing: {name}")
    return problems


def read_migration_diagnostics(conn: sqlite3.Connection) -> dict:
    """读取版本与迁移历史，供 /migrationz 使用（不含路径）。"""
    tables = _table_names(conn)
    info: dict = {
        "schema_version": None,
        "applied": [],
    }
    if "schema_meta" in tables:
        row = conn.execute(
            "SELECT schema_version, applied_count, upgraded_at "
            "FROM schema_meta WHERE id = 1"
        ).fetchone()
        if row is not None:
            info["schema_version"] = int(row["schema_version"])
            info["applied_count"] = int(row["applied_count"])
            info["upgraded_at"] = row["upgraded_at"]
    if "schema_migrations" in tables:
        info["applied"] = [
            {"version": int(r["version"]), "action": r["action"], "applied_at": r["applied_at"]}
            for r in conn.execute(
                "SELECT version, action, applied_at "
                "FROM schema_migrations ORDER BY version"
            )
        ]
    return info


# --------------------------------------------------------------------------- #
# 主入口
# --------------------------------------------------------------------------- #

# 测试可用 failure_hook(step_version, conn) 注入磁盘错误/崩溃：抛异常模拟
# 磁盘不足（事务回滚），os._exit 模拟进程被杀（下次打开由日志恢复）。
FailureHook = Callable[[int, sqlite3.Connection], None]


def prepare_database(
    db_path: Path | str,
    *,
    timeout: float = 60.0,
    failure_hook: FailureHook | None = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> PrepareResult:
    """确保数据库处于当前 schema 版本并通过结构校验。

    多实例并发调用时，只有一个实例执行升级，其余实例阻塞等待，待升级
    提交后重新读取并复核版本。返回 :class:`PrepareResult` 供诊断使用。
    """
    db_path = Path(db_path)
    deadline = time.monotonic() + timeout
    started = time.monotonic()

    while True:
        conn = connect_admin(db_path)
        try:
            state = _read_state(conn)

            if state.meta_version == CURRENT_SCHEMA_VERSION:
                problems = _verify_current_schema(conn)
                if problems:
                    raise SchemaDriftError(problems)
                return PrepareResult(
                    schema_version=CURRENT_SCHEMA_VERSION,
                    from_version=CURRENT_SCHEMA_VERSION,
                    actions=["verified"],
                    elapsed_ms=int((time.monotonic() - started) * 1000),
                )

            if state.meta_version is not None and state.meta_version > CURRENT_SCHEMA_VERSION:
                raise SchemaVersionTooNewError(
                    state.meta_version, CURRENT_SCHEMA_VERSION
                )

            if state.legacy_version is None:
                raise SchemaDriftError(
                    ["database structure is unrecognizable and cannot be migrated safely"]
                )

            start_version = (
                state.meta_version
                if state.meta_version is not None
                else state.legacy_version
            )

            try:
                conn.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError:
                # 另一个实例正持有写锁执行升级。
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise MigrationTimeoutError from None
                conn.close()
                # 随机抖动避免多实例同步重试（惊群）。
                sleeper(min(0.2, remaining / 2) + random.uniform(0.0, 0.1))
                continue

            # 拿到写锁后必须重读：等待期间别的实例可能已经提交升级。
            state = _read_state(conn)
            if state.meta_version == CURRENT_SCHEMA_VERSION:
                conn.execute("ROLLBACK")
                conn.close()
                continue
            if state.meta_version is not None and state.meta_version > CURRENT_SCHEMA_VERSION:
                conn.execute("ROLLBACK")
                raise SchemaVersionTooNewError(
                    state.meta_version, CURRENT_SCHEMA_VERSION
                )
            if state.legacy_version is None:
                conn.execute("ROLLBACK")
                raise SchemaDriftError(
                    ["database structure is unrecognizable and cannot be migrated safely"]
                )
            start_version = (
                state.meta_version
                if state.meta_version is not None
                else state.legacy_version
            )

            try:
                executed = _upgrade_locked(conn, start_version, failure_hook)
                problems = _verify_current_schema(conn)
                if problems:
                    raise SchemaDriftError(problems)
                conn.execute("COMMIT")
            except BaseException:
                # 磁盘错误、注入失败或校验不过：整次升级回滚，不留半成品。
                # os._exit() 这类硬崩溃不会走到这里，由 SQLite 日志在下次
                # 打开时自动恢复未提交事务。
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise

            actions: list[str]
            if start_version == 0:
                actions = ["initialized"]
            else:
                actions = [f"migrated_{start_version}_to_{CURRENT_SCHEMA_VERSION}"]
            return PrepareResult(
                schema_version=CURRENT_SCHEMA_VERSION,
                from_version=start_version,
                actions=actions,
                migrated_steps=executed,
                elapsed_ms=int((time.monotonic() - started) * 1000),
            )
        finally:
            conn.close()


def _upgrade_locked(
    conn: sqlite3.Connection,
    start_version: int,
    failure_hook: FailureHook | None,
) -> list[int]:
    """调用方已持有 BEGIN IMMEDIATE 事务。"""
    stamp = _utcnow_iso()

    for stmt in META_TABLE_SQL:
        conn.execute(stmt)

    executed: list[int] = []

    if start_version == 0:
        for stmt in _BASELINE_TABLES:
            conn.execute(stmt)
        for stmt in _BASELINE_INDEXES:
            conn.execute(stmt)
    else:
        for version in range(start_version + 1, CURRENT_SCHEMA_VERSION + 1):
            _sql, step = MIGRATIONS[version]
            step(conn)
            executed.append(version)
            if failure_hook is not None:
                failure_hook(version, conn)

    # 记录历史：全新库把 1..当前 版本记作 baseline；旧库接入时，起始版本
    # 之前的版本记作 baseline，实际执行的步骤记作 migrate。
    baseline_end = (
        CURRENT_SCHEMA_VERSION if start_version == 0 else start_version
    )
    for version in range(1, baseline_end + 1):
        conn.execute(
            "INSERT INTO schema_migrations(version, action, applied_at, checksum) "
            "VALUES (?, 'baseline', ?, NULL)",
            (version, stamp),
        )
    for version in executed:
        conn.execute(
            "INSERT INTO schema_migrations(version, action, applied_at, checksum) "
            "VALUES (?, 'migrate', ?, ?)",
            (version, stamp, STEP_CHECKSUMS[version]),
        )

    conn.execute(
        """
    INSERT INTO schema_meta(id, schema_version, upgraded_at, applied_count)
    VALUES (1, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
        schema_version = excluded.schema_version,
        upgraded_at    = excluded.upgraded_at,
        applied_count  = excluded.applied_count
    """,
        (CURRENT_SCHEMA_VERSION, stamp, len(executed)),
    )
    return executed


def _utcnow_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
