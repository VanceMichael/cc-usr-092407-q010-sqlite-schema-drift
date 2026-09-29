# 机场中断影响服务

本项目提供纯后端机场中断影响服务。服务接收机场关闭、延长关闭和恢复开放事件，计算受影响的既有航班与旅客，将事件链和计算结果保存到 SQLite，并通过 HTTP 接口提供查询。

示例中的机场、航班时刻和旅客数量均为合成数据。运行期间不会请求外部航班、地图或通知服务。

## 目录

- `contracts/disruption-event.schema.json`：中断事件输入契约。
- `fixtures/airports.json`：机场时区与恢复缓冲时间。
- `fixtures/flights.json`：确定性的航班计划数据，包含跨午夜样例。
- `app/`：Python 3.12 标准库实现的业务服务。
- `tests/`：计算、校验、存储和 HTTP 集成测试。
- `scripts/docker_selftest.sh`：容器化黑盒自检入口。
- `compose.yaml`：输入校验容器、业务服务和 SQLite 持久卷。

## 启动

```bash
docker compose up -d --build --wait
curl -s http://127.0.0.1:8080/healthz
docker compose down
```

SQLite 默认位于容器内的 `/data/disruptions.db`，由 `disruption-data` 卷保存。`DB_PATH`、`HOST`、`PORT` 和 `FIXTURES_DIR` 均可通过环境变量调整。

本地运行只需要 Python 3.12：

```bash
python3 -m unittest discover -s tests
DB_PATH=./data/disruptions.db PORT=8080 python3 -m app
```

## 接口

所有请求和响应均为 JSON，错误统一使用以下结构：

```json
{"error": {"code": "unknown_airport", "message": "...", "details": {}}}
```

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/v1/events` | 提交关闭、延长或恢复事件（未就绪时返回 503） |
| `GET` | `/api/v1/events/{event_id}` | 查询事件、处理状态和影响结果 |
| `GET` | `/api/v1/airports/{AIRPORT}/summary` | 查询机场影响汇总 |
| `GET` | `/api/v1/flights/affected` | 分页查询当前受影响航班 |
| `GET` | `/healthz` | 存活探针：进程与数据库连接可往返即正常 |
| `GET` | `/readyz` | 就绪探针：迁移完成且结构校验通过才返回 200 |
| `GET` | `/migrations` | 迁移诊断：版本、角色、已应用步骤或阻断原因 |

受影响航班查询支持 `airport`、`status`、`limit` 和 `offset` 参数。`status` 可取 `cancelled`、`delayed` 或 `pending_confirmation`。

## Schema 版本管理与迁移

早期服务的数据库没有版本记录，仅靠 `CREATE TABLE IF NOT EXISTS` 建表，
滚动发布接到旧文件时缺少新增列也不会报错，直到第一条写入才失败。现在：

- `schema_meta` 表记录结构版本；启动时先识别版本（空库 / v1 旧库 / 当前版本 /
  无法识别），再决定初始化或顺序升级。
- 迁移按版本顺序执行；所有待执行步骤在**同一个 `BEGIN IMMEDIATE` 事务**中
  完成，版本号最后写入。每个步骤自身幂等（先检查列、`CREATE INDEX IF NOT
  EXISTS`）。提交前会再做一次完整结构复核。进程崩溃、提交失败（如磁盘不足）
  时整批回滚，不会留下半迁移状态。
- 多实例同时启动时写锁即选举锁：只有一个实例执行升级（`role=leader`），
  其余实例等待并周期性只读复核，领导者提交后以 `role=follower` 就绪；锁
  等待超时属于瞬时状态，后续 `/readyz` 探针会自动重试引导，无需重启。
- 版本比本进程新、表/列被手工改坏、必要索引缺失或完整性检查失败时，实例
  保持 **blocked**：`/healthz` 仍返回 200（进程存活，重启无济于事），
  `/readyz` 返回 503，`/migrations` 给出结构化原因，所有 `/api/*` 业务
  请求一律返回 `503 service_unavailable`，不接流量。
- 诊断响应只包含版本号、列名、索引名等结构标识符，不包含数据库文件路径。

相关环境变量：`MIGRATION_LOCK_TIMEOUT`（等待迁移锁的秒数，默认 30）。
`MIGRATION_CRASH_AFTER_STEP=N` 仅用于中断演练：在第 N 个迁移步骤后、提交前
硬退出（退出码 27）；卷上的 `.migration-crash-sent` 哨兵保证崩溃只发生一次，
带着相同环境变量重启会正常完成迁移。

也可在容器/本地直接使用命令行工具（输出不含路径）：

```bash
python -m app.migrations inspect DB_PATH   # 只读观察，不做任何修改
python -m app.migrations upgrade DB_PATH   # 加锁执行迁移/校验
```


## 事件规则

- 所有输入时间必须携带时区，比较前统一转换为 UTC。
- `airport.closed` 创建事件链；未知结束时间可将 `effective_until` 设为 `null`。
- `airport.extended` 通过 `supersedes_event_id` 延长尚未结束的事件链。
- `airport.reopened` 结束事件链，机场在自身恢复缓冲时间结束后重新运行。
- 时间窗口采用左闭右开语义，恰好落在恢复时刻的航班不受影响。
- 同一航班分别按出发机场的计划起飞时刻和到达机场的计划到达时刻判断。
- 无法改时或所需延误超过上限的航班标记为 `cancelled`；可在上限内改时的航班标记为 `delayed`；结束时间未知时标记为 `pending_confirmation`。
- 跨午夜依据受影响机场的本地时区判定。
- `event_id` 是幂等键。相同内容重试返回原结果；相同标识携带不同内容时返回 `409 event_conflict`。
- 事件链版本必须递增，且不能继续延长已经恢复开放的事件链。

## 编译检查

```bash
python3 -m compileall -q app
```

## 验证

单元与集成测试：

```bash
python3 -m unittest discover -s tests -v
```

完整容器自检：

```bash
scripts/docker_selftest.sh
```

容器自检会依次演练：空卷构建与输入校验、幂等重放、跨午夜计算、事件链变化、
分页查询、容器重建后的数据保留；早期 v1 结构卷的原地升级与重复重启、
升级后继续幂等处理原事件；迁移事务中途硬崩溃后的回滚与再次启动恢复；
以及结构被手工改坏时"存活但永不就绪、拒绝所有业务流量"的行为，结束时
清理测试资源。

构造或检查早期 v1 数据库可使用：

```bash
python scripts/legacy_db.py build DB_PATH [FIXTURES_DIR]
python scripts/legacy_db.py inspect DB_PATH
```

原始契约与夹具也可单独校验：

```bash
docker compose up -d --build scaffold
docker compose exec scaffold sh scaffold/validate_inputs.sh
```
