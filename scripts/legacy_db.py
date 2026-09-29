#!/usr/bin/env python3
"""在卷上构造一个"早期服务留下的"版本 1 SQLite 数据库。

容器滚动升级演练用：

* 先用当前业务引擎在暂存库中生成一条 KTA 关闭事件及其影响结果；
* 删除目标文件后按 v1 结构（events 无 replay_count/created_at、无
  idx_events_airport 索引、无 schema_meta 表）重建，并只写入 v1 列。

这样启动新版本容器时必须真正完成 v1 -> v2 迁移，而迁移后事件与影响
结果必须与引擎当前的计算结果逐字节一致（幂等重放由此可严格比较）。

用法：
    python scripts/legacy_db.py build DB_PATH [FIXTURES_DIR]
    python scripts/legacy_db.py inspect DB_PATH
"""

from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from pathlib import Path

from app.config import DEFAULT_FIXTURES_DIR, load_airports, load_flights
from app.migrations import LEGACY_V1_SCHEMA, observe_database
from app.repository import Repository
from app.service import DisruptionService

# 旧版服务在 v1 时代写入的事件。独立的 event_id，窗口与正式自检夹具不重叠
# （本脚本总是在独立卷上使用）。
LEGACY_EVENT_ID = "volc-kta-legacy01"
LEGACY_PAYLOAD = {
    "event_id": LEGACY_EVENT_ID,
    "event_version": 1,
    "event_type": "airport.closed",
    "airport_code": "KTA",
    "effective_from": "2026-09-07T16:30:00Z",
    "effective_until": "2026-09-07T18:00:00Z",
    "reported_at": "2026-09-07T14:10:00Z",
    "reason": "legacy ash plume (written by v1 service)",
}

EVENT_V1_COLUMNS = [
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
IMPACT_COLUMNS = [
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


def build(target: Path, fixtures_dir: Path) -> int:
    airports = load_airports(fixtures_dir)
    flights = load_flights(fixtures_dir, airports)

    with tempfile.TemporaryDirectory() as tmp:
        staging = Path(tmp) / "staging.db"
        repo = Repository(staging)
        service = DisruptionService(repo, airports, flights)
        result = service.submit_event(dict(LEGACY_PAYLOAD))
        repo.close()

        reader = sqlite3.connect(str(staging))
        reader.row_factory = sqlite3.Row
        event_rows = [dict(r) for r in reader.execute("SELECT * FROM events")]
        impact_rows = [dict(r) for r in reader.execute("SELECT * FROM impacts")]
        reader.close()

    for suffix in ("", "-wal", "-shm"):
        path = Path(str(target) + suffix)
        path.unlink(missing_ok=True)
    target.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(target))
    conn.executescript(LEGACY_V1_SCHEMA)
    conn.executemany(
        f"INSERT INTO events ({', '.join(EVENT_V1_COLUMNS)}) "
        f"VALUES ({', '.join('?' for _ in EVENT_V1_COLUMNS)})",
        [tuple(row[c] for c in EVENT_V1_COLUMNS) for row in event_rows],
    )
    conn.executemany(
        f"INSERT INTO impacts ({', '.join(IMPACT_COLUMNS)}) "
        f"VALUES ({', '.join('?' for _ in IMPACT_COLUMNS)})",
        [tuple(row[c] for c in IMPACT_COLUMNS) for row in impact_rows],
    )
    conn.commit()

    columns = {r[1] for r in conn.execute("PRAGMA table_info(events)")}
    tables = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    event_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    impact_count = conn.execute("SELECT COUNT(*) FROM impacts").fetchone()[0]
    conn.close()

    assert "replay_count" not in columns, "legacy build must use v1 events table"
    assert "created_at" not in columns, "legacy build must use v1 events table"
    assert "schema_meta" not in tables, "legacy build predates schema_meta"

    print(
        json.dumps(
            {
                "built": str(target),
                "schema": "v1",
                "events": event_count,
                "impacts": impact_count,
                "legacy_event_id": LEGACY_EVENT_ID,
                "expected_impact_count": result["impact_count"],
            },
            indent=2,
        )
    )
    return 0


def inspect(target: Path) -> int:
    conn = sqlite3.connect(str(target))
    conn.row_factory = sqlite3.Row
    state = observe_database(conn)
    print(json.dumps(state.public_dict(), indent=2, sort_keys=True))
    return 0 if state.state != "blocked" else 3


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__)
        return 2
    command = argv[0]
    target = Path(argv[1])
    fixtures_dir = Path(argv[2]) if len(argv) > 2 else DEFAULT_FIXTURES_DIR
    if command == "build":
        return build(target, fixtures_dir)
    if command == "inspect":
        return inspect(target)
    print(f"unknown command: {command}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
