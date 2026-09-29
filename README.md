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
| `POST` | `/api/v1/events` | 提交关闭、延长或恢复事件 |
| `GET` | `/api/v1/events/{event_id}` | 查询事件、处理状态和影响结果 |
| `GET` | `/api/v1/airports/{AIRPORT}/summary` | 查询机场影响汇总 |
| `GET` | `/api/v1/flights/affected` | 分页查询当前受影响航班 |
| `GET` | `/healthz` | 存活探针：进程可应答即 200（迁移中也是 200） |
| `GET` | `/readyz` | 就绪探针：schema 当前且校验通过才 200，否则 503 |
| `GET` | `/migrationz` | 迁移诊断：当前/目标版本与升级历史，不含文件路径 |

受影响航班查询支持 `airport`、`status`、`limit` 和 `offset` 参数。`status` 可取 `cancelled`、`delayed` 或 `pending_confirmation`。

## Schema 版本与迁移

数据库结构由版本号管理，杜绝滚动发布时“新进程接上旧文件、`CREATE TABLE
IF NOT EXISTS` 静默成功、写第一条关闭事件才报错”的结构漂移：

- `schema_meta` 记录当前结构版本（当前为 `3`），`schema_migrations` 记录每个版本是基线建立（`baseline`）还是迁移得到（`migrate`）及步骤校验和。
- 启动时先在后台执行迁移，HTTP 监听立即开始，因此三个探针在整个升级期间都可应答。
- 升级步骤严格按版本号顺序执行，整段包在**一个 `BEGIN IMMEDIATE` 事务**中；每个步骤本身幂等。任一步失败（含磁盘不足）整体回滚；进程崩溃或被 SIGKILL 时由 SQLite 日志在下次打开时回滚未提交事务——不会留下“升了一半”的库。
- 多实例同时启动时，`BEGIN IMMEDIATE` 的文件级保留锁保证只有一个实例执行升级，其余实例等待并在升级提交后重新读取、复核版本再开放流量。
- 升级采用保数据方式（如“建新表—拷贝—改名”），已有事件与影响结果完整保留；无版本记录的历史结构（v1/v2/v3）会被识别并接入。
- 打开时与就绪探针上都会校验必备表、列与索引。

各阶段探针语义：

| 状态 | `/healthz` | `/readyz` | `/migrationz` | 业务接口 |
| --- | --- | --- | --- | --- |
| 迁移中 | 200 `alive` | 503 `schema_migration_in_progress` | 503 `migrating` | 503 |
| 就绪 | 200 `alive` | 200 `ready` | 200 `current` | 正常 |
| 版本过新 | 200 `alive` | 503 `schema_version_too_new` | 500 `failed` | 503 |
| 结构改坏 / 缺索引 | 200 `alive` | 503（含问题清单） | 500 `failed` | 503 |
| 存储不可用 | 200 `alive` | 503 `storage_unavailable` | 500/诊断 | 503 |

迁移失败是本进程内的终态：进程保持存活以暴露诊断、拒绝接流量，交由编排平台重启或人工介入；重启后幂等步骤会重新尝试。所有探针与错误响应只描述结构问题，**不包含数据库文件路径**。容器 `HEALTHCHECK` 与 compose 健康检查使用 `/readyz`，未就绪实例不会被接入流量。

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

容器自检会从空卷构建并启动服务，验证输入校验、幂等重放、跨午夜计算、事件链变化、分页查询以及容器重建后的数据保留，结束时清理测试资源。

原始契约与夹具也可单独校验：

```bash
docker compose up -d --build scaffold
docker compose exec scaffold sh scaffold/validate_inputs.sh
```
