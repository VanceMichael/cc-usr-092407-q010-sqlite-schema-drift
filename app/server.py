"""基于标准库 ThreadingHTTPServer 的 JSON HTTP 接口层。"""

from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

from app.errors import (
    AppError,
    BadRequestError,
    MethodNotAllowedError,
    NotFoundError,
    UnsupportedMediaTypeError,
)
from app.lifecycle import (
    PHASE_MIGRATING,
    PHASE_READY,
    PHASE_STARTING,
    Application,
)

MAX_BODY_BYTES = 64 * 1024
DEFAULT_LIMIT = 50
MAX_LIMIT = 200


class AppState:
    def __init__(self, app: Application):
        self.app = app


def make_handler(state: AppState) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "DisruptionService/1.0"
        protocol_version = "HTTP/1.1"

        # Quieter access log; comment out to restore defaults.
        def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
            return

        # ------------------------------------------------------------------ #
        # Routing
        # ------------------------------------------------------------------ #

        def do_GET(self) -> None:  # noqa: N802
            self._dispatch("GET")

        def do_POST(self) -> None:  # noqa: N802
            self._dispatch("POST")

        def do_PUT(self) -> None:  # noqa: N802
            self._dispatch("PUT")

        def do_DELETE(self) -> None:  # noqa: N802
            self._dispatch("DELETE")

        def do_PATCH(self) -> None:  # noqa: N802
            self._dispatch("PATCH")

        def _dispatch(self, method: str) -> None:
            try:
                parts = urlsplit(self.path)
                path = parts.path.rstrip("/") or "/"
                query = parse_qs(parts.query)

                if path == "/healthz":
                    self._require_method(method, "GET", path)
                    # 存活探针：进程能响应即存活。迁移进行中或迁移失败都不
                    # 应被编排平台杀死（失败时靠就绪探针摘流量并暴露诊断）。
                    self._send_json(
                        200, {"status": "alive", "phase": state.app.status.phase}
                    )
                    return

                if path == "/readyz":
                    self._require_method(method, "GET", path)
                    self._readiness()
                    return

                if path == "/migrationz":
                    self._require_method(method, "GET", path)
                    self._migration_diagnostics()
                    return

                if path == "/api/v1" or path == "/":
                    self._require_method(method, "GET", path)
                    self._send_json(
                        200,
                        {
                            "service": "airport-disruption",
                            "endpoints": [
                                "POST /api/v1/events",
                                "GET  /api/v1/events/{event_id}",
                                "GET  /api/v1/airports/{airport_code}/summary",
                                "GET  /api/v1/flights/affected",
                                "GET  /healthz",
                                "GET  /readyz",
                                "GET  /migrationz",
                            ],
                        },
                    )
                    return

                # 业务接口必须在迁移完成、结构复核通过后才允许接流量。
                service = self._require_service()
                if service is None:
                    return

                match = re.fullmatch(r"/api/v1/events/([A-Za-z0-9-]+)", path)
                if match:
                    self._require_method(method, "GET", path)
                    self._send_json(200, service.event_status(match.group(1)))
                    return

                match = re.fullmatch(
                    r"/api/v1/airports/([A-Z]{3})/summary", path
                )
                if match:
                    self._require_method(method, "GET", path)
                    self._send_json(200, service.airport_summary(match.group(1)))
                    return

                if path == "/api/v1/flights/affected":
                    self._require_method(method, "GET", path)
                    self._send_json(200, self._affected_flights(query, service))
                    return

                if path == "/api/v1/events":
                    self._require_method(method, "POST", path)
                    payload = self._read_json_body()
                    self._send_json(201, service.submit_event(payload))
                    return

                raise NotFoundError(f"No route for {method} {path}")

            except AppError as exc:
                self._send_json(exc.status, exc.to_dict())
            except Exception:  # never leak a stack trace to clients
                import traceback

                traceback.print_exc()
                self._send_json(
                    500,
                    {"error": {"code": "internal_error", "message": "Internal server error"}},
                )

        # ------------------------------------------------------------------ #
        # Readiness / migration probes
        # ------------------------------------------------------------------ #

        def _readiness(self) -> None:
            status = state.app.status
            if status.phase in (PHASE_STARTING, PHASE_MIGRATING):
                self._send_json(
                    503,
                    {
                        "status": "not_ready",
                        "reason": "schema_migration_in_progress",
                        "phase": status.phase,
                    },
                    {"Retry-After": "1"},
                )
                return
            if status.phase != PHASE_READY:
                self._send_json(
                    503,
                    {
                        "status": "not_ready",
                        "reason": status.code or "startup_failed",
                        "message": status.message or "service failed to start",
                        "problems": status.problems,
                    },
                    {"Retry-After": "5"},
                )
                return

            problems = state.app.ready_problems()
            if problems:
                # 运行期结构被破坏或存储故障：立刻摘流量，但信息不含路径。
                self._send_json(
                    503,
                    {
                        "status": "not_ready",
                        "reason": "storage_unavailable",
                        "problems": problems,
                    },
                    {"Retry-After": "5"},
                )
                return
            self._send_json(
                200,
                {
                    "status": "ready",
                    "schema_version": status.prepared.schema_version
                    if status.prepared
                    else None,
                },
            )

        def _migration_diagnostics(self) -> None:
            status = state.app.status
            body = state.app.migration_diagnostics()
            if status.phase in (PHASE_STARTING, PHASE_MIGRATING):
                self._send_json(503, {"status": "migrating", **body})
            elif status.phase == PHASE_READY:
                self._send_json(200, {"status": "current", **body})
            else:
                # 终态错误（版本过新 / 结构漂移 / 存储不可用）。
                self._send_json(500, {"status": "failed", **body})

        def _require_service(self):
            """未就绪时对所有业务请求返回 503；就绪则返回业务服务。

            阶段在启动结束后即为稳定终态；运行期的存储/结构退化由
            ``/readyz`` 探针周期性发现并触发编排平台摘流量。
            """
            status = state.app.status
            if status.phase == PHASE_READY:
                return state.app.service

            if status.phase in (PHASE_STARTING, PHASE_MIGRATING):
                self._send_json(
                    503,
                    {
                        "error": {
                            "code": "service_not_ready",
                            "message": "service is starting; schema migration in progress",
                        }
                    },
                    {"Retry-After": "1"},
                )
                return None

            self._send_json(
                503,
                {
                    "error": {
                        "code": "service_not_ready",
                        "message": "service is not ready to serve requests",
                        "details": {
                            "reason": status.code or "startup_failed",
                            "problems": status.problems,
                        },
                    }
                },
                {"Retry-After": "5"},
            )
            return None

        def _require_method(self, method: str, expected: str, path: str) -> None:
            if method != expected:
                raise MethodNotAllowedError(
                    f"{method} is not allowed for {path}; use {expected}",
                    {"allowed": expected},
                )

        # ------------------------------------------------------------------ #
        # Request/response helpers
        # ------------------------------------------------------------------ #

        def _read_json_body(self) -> Any:
            ctype = self.headers.get("Content-Type", "")
            if not ctype.split(";")[0].strip().lower() == "application/json":
                raise UnsupportedMediaTypeError(
                    "Content-Type must be application/json",
                    {"received_content_type": ctype or None},
                )
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self.close_connection = True
                raise BadRequestError("Invalid Content-Length header") from None
            if length <= 0:
                raise BadRequestError("Request body is empty")
            if length > MAX_BODY_BYTES:
                # Do not drain an oversized body; drop the connection so
                # unread bytes cannot corrupt the next keep-alive request.
                self.close_connection = True
                raise BadRequestError(
                    f"Request body exceeds {MAX_BODY_BYTES} bytes",
                    {"max_bytes": MAX_BODY_BYTES},
                )
            raw = self.rfile.read(length)
            try:
                payload = json.loads(raw.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise BadRequestError(
                    "Request body is not valid JSON", {"detail": str(exc)}
                ) from None
            return payload

        def _affected_flights(self, query: dict[str, list[str]], service) -> dict[str, Any]:
            def one(name: str) -> str | None:
                values = query.get(name)
                if values is None:
                    return None
                if len(values) > 1:
                    raise BadRequestError(
                        f"Query parameter '{name}' must be provided once"
                    )
                return values[0]

            limit = self._parse_int(one("limit"), DEFAULT_LIMIT, "limit", 1, MAX_LIMIT)
            offset = self._parse_int(one("offset"), 0, "offset", 0, 100_000)
            return service.affected_flights(
                airport=one("airport"),
                status=one("status"),
                limit=limit,
                offset=offset,
            )

        @staticmethod
        def _parse_int(
            raw: str | None, default: int, name: str, minimum: int, maximum: int
        ) -> int:
            if raw is None:
                return default
            try:
                value = int(raw)
            except ValueError:
                raise BadRequestError(
                    f"Query parameter '{name}' must be an integer",
                    {"received": raw},
                ) from None
            if not minimum <= value <= maximum:
                raise BadRequestError(
                    f"Query parameter '{name}' must be between {minimum} and {maximum}",
                    {"received": value},
                )
            return value

        def _send_json(
            self,
            status: int,
            body: dict[str, Any],
            headers: dict[str, str] | None = None,
        ) -> None:
            data = json.dumps(body, ensure_ascii=False, sort_keys=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            for name, value in (headers or {}).items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(data)

    return Handler


def build_server(host: str, port: int, app: Application) -> ThreadingHTTPServer:
    state = AppState(app)
    server = ThreadingHTTPServer((host, port), make_handler(state))
    return server
