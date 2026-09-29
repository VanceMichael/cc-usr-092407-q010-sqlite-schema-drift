"""应用入口：加载夹具、打开数据库并启动 HTTP 服务。

即使数据库迁移结论为 blocked（版本过新、结构漂移等），HTTP 服务依然启动：
存活、就绪与迁移诊断端点必须可回答，由就绪探针和流量闸门阻止该实例接流量。
"""

from __future__ import annotations

import sys

from app.config import Config, load_airports, load_flights
from app.repository import Repository
from app.server import build_server
from app.service import DisruptionService


def main() -> int:
    config = Config.from_env()
    airports = load_airports(config.fixtures_dir)
    flights = load_flights(config.fixtures_dir, airports)
    repo = Repository(config.db_path, migration_lock_timeout=config.migration_lock_timeout)
    service = DisruptionService(repo, airports, flights)

    state = repo.migration_state
    if state.ready:
        print(
            f"airport-disruption schema ready: version {state.current_version} "
            f"(role={state.role}"
            + (f", applied={len(state.applied_steps)} step(s)" if state.applied_steps else "")
            + ")",
            flush=True,
        )
    else:
        # 诊断行只含结构标识，不含数据库路径。
        print(
            f"airport-disruption schema BLOCKED: {state.state} "
            f"version={state.current_version} reason={state.reason} "
            f"issues={list(state.issues)}; serving diagnostics only",
            file=sys.stderr,
            flush=True,
        )

    server = build_server(config.host, config.port, service)
    print(
        f"airport-disruption service listening on {config.host}:{config.port} "
        f"(airports={len(airports)}, flights={len(flights)}, ready={state.ready})",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        repo.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
