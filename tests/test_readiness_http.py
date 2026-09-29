"""健康、就绪与迁移诊断端点的 HTTP 行为测试。

核心契约：
* /healthz 只反映进程存活——结构被阻止时仍然 200；
* /readyz 反映迁移就绪——未就绪返回 503；
* /migrations 给出结构化诊断（版本、原因、问题清单），但不含文件路径；
* 未就绪时所有业务路径一律 503 service_unavailable，不处理任何事件。
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

from app.config import ROOT, load_airports, load_flights
from app.migrations import (
    REASON_MISSING_INDEX,
    REASON_UNSUPPORTED_VERSION,
    bootstrap_database,
)
from app.repository import Repository
from app.server import build_server
from app.service import DisruptionService
from tests.test_http import _request
from tests.test_migrations import MigrationFixture, aps_close

FIXTURES_DIR = ROOT / "fixtures"


class BlockedServiceHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.airports = load_airports(FIXTURES_DIR)
        self.flights = load_flights(FIXTURES_DIR, self.airports)

        # 准备一个结构被手工改坏（缺必要索引）的当前版本库。
        self.db_path = self.tmp / "blocked.db"
        conn = sqlite3.connect(str(self.db_path), isolation_level=None)
        state = bootstrap_database(conn)
        self.assertTrue(state.ready)
        conn.execute("DROP INDEX idx_impacts_flight")
        conn.commit()
        conn.close()

        self.repo = Repository(self.db_path)
        self.assertFalse(self.repo.ready, "fixture must start blocked")
        self.service = DisruptionService(self.repo, self.airports, self.flights)
        self.server: ThreadingHTTPServer = build_server("127.0.0.1", 0, self.service)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.repo.close()
        self._tmp.cleanup()

    def test_liveness_stays_ok_when_blocked(self) -> None:
        status, body = _request("GET", f"{self.base}/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ok")

    def test_readiness_reports_not_ready(self) -> None:
        status, body = _request("GET", f"{self.base}/readyz")
        self.assertEqual(status, 503)
        self.assertEqual(body["status"], "not_ready")
        self.assertEqual(body["migration"]["state"], "blocked")
        self.assertEqual(body["migration"]["reason"], REASON_MISSING_INDEX)
        self.assertIn("missing index: idx_impacts_flight", body["migration"]["issues"])

    def test_migrations_diagnostic_is_structured_and_path_free(self) -> None:
        status, body = _request("GET", f"{self.base}/migrations")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "blocked")
        migration = body["migration"]
        self.assertEqual(migration["state"], "blocked")
        self.assertEqual(migration["reason"], REASON_MISSING_INDEX)
        self.assertIn("missing index: idx_impacts_flight", migration["issues"])
        rendered = json.dumps(body)
        self.assertNotIn(str(self.db_path), rendered)
        self.assertNotIn(self._tmp.name, rendered)

    def test_business_traffic_is_rejected_while_blocked(self) -> None:
        payload = {
            "event_id": "evt-blocked00001",
            "event_version": 1,
            "event_type": "airport.closed",
            "airport_code": "APS",
            "effective_from": "2026-09-07T15:00:00Z",
            "effective_until": "2026-09-07T19:00:00Z",
            "reported_at": "2026-09-07T14:00:00Z",
        }
        status, body = _request("POST", f"{self.base}/api/v1/events", payload)
        self.assertEqual(status, 503)
        self.assertEqual(body["error"]["code"], "service_unavailable")
        # 挡下的请求不得落库。
        status, _ = _request("GET", f"{self.base}/api/v1/events/evt-blocked00001")
        self.assertEqual(status, 503)
        # 查询类业务路径同样被挡。
        status, _ = _request("GET", f"{self.base}/api/v1/flights/affected")
        self.assertEqual(status, 503)
        status, _ = _request("GET", f"{self.base}/api/v1/airports/APS/summary")
        self.assertEqual(status, 503)

    def test_diagnostic_remains_available_under_blocked_state(self) -> None:
        for path in ("/healthz", "/readyz", "/migrations"):
            status, _ = _request("GET", f"{self.base}{path}")
            self.assertIn(status, (200, 503), path)


class FutureVersionHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.db_path = self.tmp / "future.db"
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        conn.execute("INSERT INTO schema_meta VALUES ('schema_version', '42')")
        conn.commit()
        conn.close()
        self.repo = Repository(self.db_path)
        self.service = DisruptionService(
            self.repo,
            load_airports(FIXTURES_DIR),
            load_flights(FIXTURES_DIR, load_airports(FIXTURES_DIR)),
        )
        self.server = build_server("127.0.0.1", 0, self.service)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.repo.close()
        self._tmp.cleanup()

    def test_newer_version_blocks_ready_but_not_liveness(self) -> None:
        status, health = _request("GET", f"{self.base}/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(health["status"], "ok")
        status, ready = _request("GET", f"{self.base}/readyz")
        self.assertEqual(status, 503)
        self.assertEqual(ready["migration"]["reason"], REASON_UNSUPPORTED_VERSION)
        self.assertEqual(ready["migration"]["current_version"], 42)
        # 版本号不是路径信息，可以出现在诊断里；文件位置不可以。
        self.assertNotIn(self._tmp.name, json.dumps(ready))


class LegacyUpgradeOverHttpTest(unittest.TestCase):
    """v1 旧卷接入新版本二进制：启动即自动升级，随后正常服务原事件。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.airports = load_airports(FIXTURES_DIR)
        self.flights = load_flights(FIXTURES_DIR, self.airports)
        self.db_path = self.tmp / "legacy.db"
        self.payload = aps_close()
        self.original = MigrationFixture(self.tmp).build_legacy_with_event(
            self.db_path, self.payload
        )
        self.repo = Repository(self.db_path)
        self.service = DisruptionService(self.repo, self.airports, self.flights)
        self.server = build_server("127.0.0.1", 0, self.service)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.repo.close()
        self._tmp.cleanup()

    def test_auto_migrate_then_event_query_and_idempotent_replay(self) -> None:
        status, ready = _request("GET", f"{self.base}/readyz")
        self.assertEqual(status, 200)
        self.assertEqual(ready["schema_version"], 2)

        status, fetched = _request(
            "GET", f"{self.base}/api/v1/events/{self.payload['event_id']}"
        )
        self.assertEqual(status, 200)
        self.assertEqual(fetched["impacts"], self.original["impacts"])

        status, replayed = _request(
            "POST", f"{self.base}/api/v1/events", dict(self.payload)
        )
        self.assertEqual(status, 201)
        self.assertEqual(replayed["processing_state"], "replayed")
        self.assertEqual(replayed["impacts"], self.original["impacts"])

        status, status_body = _request(
            "GET", f"{self.base}/api/v1/events/{self.payload['event_id']}"
        )
        self.assertEqual(status_body["processing"]["replay_count"], 1)


class ReadyServiceHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmp.name) / "ready.db"
        airports = load_airports(FIXTURES_DIR)
        self.repo = Repository(self.db_path)
        self.service = DisruptionService(
            self.repo, airports, load_flights(FIXTURES_DIR, airports)
        )
        self.server = build_server("127.0.0.1", 0, self.service)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.repo.close()
        self._tmp.cleanup()

    def test_readiness_and_diagnostics_ok(self) -> None:
        status, ready = _request("GET", f"{self.base}/readyz")
        self.assertEqual(status, 200)
        self.assertEqual(ready["status"], "ready")
        self.assertEqual(ready["schema_version"], 2)

        status, diag = _request("GET", f"{self.base}/migrations")
        self.assertEqual(status, 200)
        self.assertEqual(diag["status"], "ok")
        self.assertEqual(diag["migration"]["state"], "ready")

    def test_healthz_remains_supported(self) -> None:
        status, body = _request("GET", f"{self.base}/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ok")

    def test_readyz_method_not_allowed(self) -> None:
        status, body = _request("POST", f"{self.base}/readyz")
        self.assertEqual(status, 405)
        self.assertEqual(body["error"]["code"], "method_not_allowed")


if __name__ == "__main__":
    unittest.main()
