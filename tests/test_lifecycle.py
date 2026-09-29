"""启动生命周期与三探针（存活/就绪/迁移诊断）的 HTTP 集成测试。

覆盖：迁移中三端点各自状态与流量闸、迁移失败（版本过新/漂移）阻止就绪且
不泄露路径、从真实旧版库后台升级到就绪后继续幂等处理原事件。
"""

from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

from app.config import ROOT, load_airports, load_flights
from app.lifecycle import (
    PHASE_ERROR,
    PHASE_MIGRATING,
    PHASE_READY,
    Application,
)
from app.migrations import (
    SchemaDriftError,
    SchemaVersionTooNewError,
    prepare_database,
)
from app.server import build_server
from tests.support import base_event
from tests.test_migrations import build_legacy_db

FIXTURES_DIR = ROOT / "fixtures"


def _request(method: str, url: str, body=None):
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


class LifecycleHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmp.name) / "lifecycle.db"
        self.airports = load_airports(FIXTURES_DIR)
        self.flights = load_flights(FIXTURES_DIR, self.airports)
        self._apps: list[Application] = []
        self._servers: list[ThreadingHTTPServer] = []
        self._threads: list[threading.Thread] = []

    def tearDown(self) -> None:
        for server in self._servers:
            server.shutdown()
            server.server_close()
        for thread in self._threads:
            thread.join(timeout=5)
        for app in self._apps:
            app.shutdown()
        self._tmp.cleanup()

    def _serve(self, app: Application) -> str:
        server = build_server("127.0.0.1", 0, app)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self._apps.append(app)
        self._servers.append(server)
        self._threads.append(thread)
        return f"http://127.0.0.1:{server.server_address[1]}"

    def _new_app(self, **kwargs) -> Application:
        return Application(self.db_path, self.airports, self.flights, **kwargs)

    # ------------------------------------------------------------------ #
    # 迁移中
    # ------------------------------------------------------------------ #

    def test_probes_and_traffic_gate_while_migrating(self) -> None:
        gate = threading.Event()

        def slow_prepare(path, *, timeout):
            gate.wait(timeout=10)
            return prepare_database(path, timeout=timeout)

        app = self._new_app(prepare_func=slow_prepare)
        app.start_async()
        base = self._serve(app)

        self.assertEqual(app.wait_for_phase(timeout=2), PHASE_MIGRATING)

        # 存活探针始终可应答。
        status, body = _request("GET", f"{base}/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "alive")
        self.assertEqual(body["phase"], PHASE_MIGRATING)

        # 就绪探针明确“迁移中”，区别于失败。
        status, body = _request("GET", f"{base}/readyz")
        self.assertEqual(status, 503)
        self.assertEqual(body["reason"], "schema_migration_in_progress")

        # 迁移诊断同样区分状态。
        status, body = _request("GET", f"{base}/migrationz")
        self.assertEqual(status, 503)
        self.assertEqual(body["status"], "migrating")

        # 业务流量在迁移期间一律被拒绝，绝不带病处理。
        status, body = _request("POST", f"{base}/api/v1/events", base_event())
        self.assertEqual(status, 503)
        self.assertEqual(body["error"]["code"], "service_not_ready")
        status, _ = _request(
            "GET", f"{base}/api/v1/events/{base_event()['event_id']}"
        )
        self.assertEqual(status, 503)

        # 放行迁移 -> 就绪，业务恢复。
        gate.set()
        self.assertEqual(app.wait_for_phase(timeout=5), PHASE_READY)
        status, body = _request("GET", f"{base}/readyz")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ready")

    # ------------------------------------------------------------------ #
    # 迁移失败：版本过新 / 结构漂移
    # ------------------------------------------------------------------ #

    def test_too_new_version_blocks_readiness_keeps_liveness(self) -> None:
        secret = self._tmp.name

        def fail(path, *, timeout):
            raise SchemaVersionTooNewError(99, 3)

        app = self._new_app(prepare_func=fail)
        app.start_async()
        base = self._serve(app)
        self.assertEqual(app.wait_for_phase(timeout=2), PHASE_ERROR)

        status, body = _request("GET", f"{base}/healthz")
        self.assertEqual(status, 200)  # 进程仍存活，便于诊断

        status, body = _request("GET", f"{base}/readyz")
        self.assertEqual(status, 503)
        self.assertEqual(body["reason"], "schema_version_too_new")

        status, body = _request("GET", f"{base}/migrationz")
        self.assertEqual(status, 500)
        self.assertEqual(body["status"], "failed")
        self.assertEqual(body["code"], "schema_version_too_new")

        status, body = _request("POST", f"{base}/api/v1/events", base_event())
        self.assertEqual(status, 503)
        # 任何响应都不得泄露文件系统路径。
        self.assertNotIn(secret, json.dumps(body))

    def test_schema_drift_blocks_readiness_without_path_leak(self) -> None:
        secret = str(self.db_path)

        def fail(path, *, timeout):
            raise SchemaDriftError(["events table is missing column: replay_count"])

        app = self._new_app(prepare_func=fail)
        app.start_async()
        base = self._serve(app)
        self.assertEqual(app.wait_for_phase(timeout=2), PHASE_ERROR)

        for path in ("/readyz", "/migrationz", "/api/v1/flights/affected"):
            status, body = _request("GET", f"{base}{path}")
            self.assertEqual(status, 503 if path == "/readyz" else (500 if path == "/migrationz" else 503))
            self.assertNotIn(secret, json.dumps(body))
            self.assertNotIn(self._tmp.name, json.dumps(body))

    # ------------------------------------------------------------------ #
    # 真实旧版库后台升级 + 继续幂等处理
    # ------------------------------------------------------------------ #

    def test_legacy_v1_boot_upgrades_and_continues_idempotently(self) -> None:
        # 用最早的 v1 结构（events 缺列、impacts 缺列）启动。
        build_legacy_db(self.db_path, 1)
        legacy_id = base_event()["event_id"]

        app = self._new_app()
        app.start_async()
        base = self._serve(app)
        self.assertEqual(app.wait_for_phase(timeout=10), PHASE_READY)

        # 原有关闭事件及其影响完整保留。
        status, fetched = _request("GET", f"{base}/api/v1/events/{legacy_id}")
        self.assertEqual(status, 200)
        self.assertEqual(len(fetched["impacts"]), 1)
        self.assertEqual(
            fetched["impacts"][0]["flight_id"], "AX-410-20260907"
        )

        # 旧版写入时没有 replay_count；重复提交走幂等重放并自增计数。
        status, replayed = _request("POST", f"{base}/api/v1/events", base_event())
        self.assertEqual(status, 201)
        self.assertEqual(replayed["processing_state"], "replayed")
        self.assertEqual(len(replayed["impacts"]), 1)

        # 关闭事件所需的新列在升级后可正常写入（这是漂移事故的原始触发点）。
        status, status_body = _request("GET", f"{base}/api/v1/events/{legacy_id}")
        self.assertEqual(status_body["processing"]["replay_count"], 1)

        # 一条全新事件也能处理。
        fresh = base_event(
            event_id="evt-after-migration1",
            airport_code="BSR",
            effective_from="2026-09-07T15:00:00Z",
            effective_until="2026-09-07T16:00:00Z",
        )
        status, created = _request("POST", f"{base}/api/v1/events", fresh)
        self.assertEqual(status, 201)
        self.assertEqual(created["processing_state"], "processed")
        self.assertGreaterEqual(created["impact_count"], 1)

    def test_runtime_schema_damage_is_detected_by_readiness(self) -> None:
        app = self._new_app()
        app.start_async()
        base = self._serve(app)
        self.assertEqual(app.wait_for_phase(timeout=10), PHASE_READY)

        status, _ = _request("GET", f"{base}/readyz")
        self.assertEqual(status, 200)

        # 模拟运行期手工破坏：删除必要索引。
        import sqlite3

        conn = sqlite3.connect(str(self.db_path))
        conn.execute("DROP INDEX idx_impacts_flight")
        conn.commit()
        conn.close()

        status, body = _request("GET", f"{base}/readyz")
        self.assertEqual(status, 503)
        self.assertEqual(body["reason"], "storage_unavailable")
        self.assertTrue(
            any("idx_impacts_flight" in p for p in body["problems"])
        )
        self.assertNotIn(str(self.db_path), json.dumps(body))

    def test_repeated_restarts_stay_ready_and_preserve_data(self) -> None:
        build_legacy_db(self.db_path, 2)
        legacy_id = base_event()["event_id"]

        for _ in range(3):
            app = self._new_app()
            app.start_async()
            base = self._serve(app)
            self.assertEqual(app.wait_for_phase(timeout=10), PHASE_READY)
            status, body = _request("GET", f"{base}/readyz")
            self.assertEqual(status, 200)
            status, fetched = _request("GET", f"{base}/api/v1/events/{legacy_id}")
            self.assertEqual(status, 200)
            self.assertEqual(len(fetched["impacts"]), 1)
            # 关停前先把 HTTP 层摘掉，避免下一轮端口对象堆积影响判断。
            self._servers[-1].shutdown()
            self._servers[-1].server_close()
            self._threads[-1].join(timeout=5)
            app.shutdown()


if __name__ == "__main__":
    unittest.main()
