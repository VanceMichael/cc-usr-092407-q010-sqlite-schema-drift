"""应用入口：加载夹具、在后台迁移数据库并启动 HTTP 服务。

HTTP 监听先于迁移完成：存活/就绪/迁移诊断探针在结构升级期间就能应答。
业务服务只有在 schema 迁移并校验成功后才会被装配，未就绪时业务接口拒绝
流量，保证结构不对时绝不接请求。
"""

from __future__ import annotations

import sys
import threading

from app.config import Config, load_airports, load_flights
from app.lifecycle import Application, PHASE_ERROR, PHASE_READY
from app.server import build_server


def main() -> int:
    config = Config.from_env()
    airports = load_airports(config.fixtures_dir)
    flights = load_flights(config.fixtures_dir, airports)

    app = Application(config.db_path, airports, flights)
    app.start_async()

    server = build_server(config.host, config.port, app)
    server_thread = threading.Thread(
        target=server.serve_forever, name="http-server", daemon=True
    )
    server_thread.start()
    print(
        f"airport-disruption service listening on {config.host}:{config.port} "
        f"(schema preparation running in background)",
        flush=True,
    )

    # 记录首个稳定阶段；无论就绪还是失败，HTTP 探针都持续可应答。
    phase = app.wait_for_phase(timeout=None)
    if phase == PHASE_READY:
        status = app.status
        print(
            f"schema ready at version {status.prepared.schema_version} "
            f"({', '.join(status.prepared.actions)})",
            flush=True,
        )
    elif phase == PHASE_ERROR:
        status = app.status
        print(
            f"schema preparation failed: {status.code}: {status.message} "
            f"problems={status.problems} - serving diagnostics only, "
            "not accepting traffic",
            file=sys.stderr,
            flush=True,
        )

    try:
        while server_thread.is_alive():
            server_thread.join(timeout=1.0)
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        server_thread.join(timeout=2)
        app.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
