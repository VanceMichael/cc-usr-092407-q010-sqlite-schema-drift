"""进程启动生命周期：后台迁移、状态机与服务装配。

HTTP 服务器在迁移开始前就监听，因此存活/就绪/迁移诊断探针在整个启动过程
中都可应答；业务服务只有在 schema 迁移并校验成功后才会被构造，未就绪期间
所有业务接口统一拒绝，保证结构不对时绝不接流量。

状态机：

    starting -> migrating -> ready
                         \\-> error （版本过新 / 结构漂移 / 迁移失败 / 存储故障）

``error`` 为终态（本进程内不自动重试），进程保持存活以暴露诊断，交由编排
平台重启或人工介入；重启后迁移步骤可幂等重跑。
"""

from __future__ import annotations

import sqlite3
import threading
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.migrations import (
    CURRENT_SCHEMA_VERSION,
    MigrationError,
    PrepareResult,
    prepare_database,
    read_migration_diagnostics,
)
from app.models import Airport, Flight
from app.repository import Repository
from app.service import DisruptionService

PHASE_STARTING = "starting"
PHASE_MIGRATING = "migrating"
PHASE_READY = "ready"
PHASE_ERROR = "error"


@dataclass
class Status:
    phase: str = PHASE_STARTING
    # 机器可读原因码，仅在 error 下非空。
    code: str | None = None
    # 面向客户端的安全信息，绝不包含文件系统路径。
    message: str | None = None
    problems: list[str] = field(default_factory=list)
    prepared: PrepareResult | None = None

    def to_public_dict(self) -> dict[str, Any]:
        body: dict[str, Any] = {"phase": self.phase}
        if self.code:
            body["code"] = self.code
        if self.message:
            body["message"] = self.message
        if self.problems:
            body["problems"] = list(self.problems)
        if self.prepared is not None:
            body["migration"] = {
                "from_version": self.prepared.from_version,
                "schema_version": self.prepared.schema_version,
                "steps": list(self.prepared.migrated_steps),
                "actions": list(self.prepared.actions),
                "elapsed_ms": self.prepared.elapsed_ms,
            }
        return body


class Application:
    """持有迁移状态、存储连接与业务服务的进程级对象。"""

    def __init__(
        self,
        db_path: Path | str,
        airports: dict[str, Airport],
        flights: dict[str, Flight],
        *,
        migration_timeout: float = 60.0,
        prepare_func=prepare_database,
    ):
        self._db_path = Path(db_path)
        self._airports = airports
        self._flights = flights
        self._migration_timeout = migration_timeout
        self._prepare_func = prepare_func
        self._condition = threading.Condition()
        self._status = Status(phase=PHASE_STARTING)
        self._service: DisruptionService | None = None
        self._repo: Repository | None = None
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    def start_async(self) -> None:
        self._thread = threading.Thread(
            target=self._bootstrap, name="schema-bootstrap", daemon=True
        )
        self._thread.start()

    @classmethod
    def prepared(
        cls,
        db_path: Path | str,
        airports: dict[str, Airport],
        flights: dict[str, Flight],
        *,
        migration_timeout: float = 60.0,
    ) -> "Application":
        """同步完成迁移并返回处于 ready 的应用（测试与嵌入场景使用）。

        迁移或校验失败时直接抛出对应异常，与后台路径使用同一套逻辑。
        """
        app = cls(
            db_path, airports, flights, migration_timeout=migration_timeout
        )
        result = prepare_database(app._db_path, timeout=migration_timeout)
        app._repo = Repository(app._db_path)
        app._service = DisruptionService(app._repo, app._airports, app._flights)
        app._status = Status(phase=PHASE_READY, prepared=result)
        return app

    def wait_for_phase(self, timeout: float | None = None) -> str:
        with self._condition:
            if self._status.phase not in (PHASE_STARTING, PHASE_MIGRATING):
                return self._status.phase
            self._condition.wait(timeout=timeout)
            return self._status.phase

    def _set_status(self, status: Status) -> None:
        with self._condition:
            self._status = status
            self._condition.notify_all()

    def _bootstrap(self) -> None:
        self._set_status(Status(phase=PHASE_MIGRATING))
        try:
            prepared = self._prepare_func(
                self._db_path, timeout=self._migration_timeout
            )
            # 迁移已提交；Repository 构造时再做一次结构复核。
            repo = Repository(self._db_path)
        except MigrationError as exc:
            self._set_status(
                Status(
                    phase=PHASE_ERROR,
                    code=exc.code,
                    message=exc.message,
                    problems=list(exc.problems),
                )
            )
            return
        except sqlite3.Error:
            # sqlite 异常文本可能包含文件路径，不能回给客户端。
            traceback.print_exc()
            self._set_status(
                Status(
                    phase=PHASE_ERROR,
                    code="storage_unavailable",
                    message="database storage is not available",
                )
            )
            return
        except Exception:  # 防御：任何意外都不能带病就绪
            traceback.print_exc()
            self._set_status(
                Status(
                    phase=PHASE_ERROR,
                    code="migration_failed",
                    message="schema migration failed unexpectedly",
                )
            )
            return

        self._repo = repo
        self._service = DisruptionService(repo, self._airports, self._flights)
        self._set_status(Status(phase=PHASE_READY, prepared=prepared))

    def shutdown(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=2)
        if self._repo is not None:
            self._repo.close()
            self._repo = None

    # ------------------------------------------------------------------ #
    # Accessors used by the HTTP layer
    # ------------------------------------------------------------------ #

    @property
    def status(self) -> Status:
        with self._condition:
            return self._status

    @property
    def service(self) -> DisruptionService | None:
        return self._service

    @property
    def repo(self) -> Repository | None:
        return self._repo

    def ready_problems(self) -> list[str]:
        if self._repo is None:
            return ["storage is not initialised"]
        try:
            return self._repo.ready_problems()
        except sqlite3.Error:
            return ["storage is not responding"]

    def migration_diagnostics(self) -> dict[str, Any]:
        """供 /migrationz；任何阶段都可调用，且不暴露路径。

        始终用只读短连接读取版本表，避免诊断探针与业务争用写锁。
        """
        body = self.status.to_public_dict()
        body["target_version"] = CURRENT_SCHEMA_VERSION
        body.setdefault("schema_version", None)

        if self._db_path.exists():
            conn = sqlite3.connect(
                f"file:{self._db_path}?mode=ro", uri=True, timeout=1
            )
            try:
                conn.row_factory = sqlite3.Row
                body.update(read_migration_diagnostics(conn))
            except sqlite3.Error:
                pass
            finally:
                conn.close()
        return body
