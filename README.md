# 施工许可公示采集 API

基于 **FastAPI + httpx + SQLite（aiosqlite）** 的施工许可公示采集服务，
配套一个**本地模拟公示站**，全部演示与测试均在本机回环完成，无需访问外网。

## 1. 功能与需求对照

| 需求 | 实现 |
| --- | --- |
| 从多页目录抓取详情 | 逐页拉取分页目录（模拟站固定 4 页、38 条目录条目），再按条目并发抓取详情 |
| 保存许可号、项目名、地址、发证日期 | `permit_records` 表保存 `permit_no / project_name / address / issue_date`，另附来源页、状态、尝试次数、错误信息 |
| 启动、暂停、恢复、查询进度和记录 | `POST /start`、`POST /pause`、`POST /resume`、`GET /progress`、`GET /records` |
| 并发采集详情，每任务最多 2 个在途请求 | 每轮采集独立 `asyncio.Semaphore(MAX_INFLIGHT)`，默认 2，目录与详情请求统一受限 |
| 跨页重复许可只留一条 | 记录以 `(task_id, permit_no)` 为主键，重复目录条目只合并来源页 `source_pages` |
| 暂停响应返回后不得继续写入 | 暂停即换代 `run_id` 并摘除内存控制块；旧轮次写库前的代际校验全部失败 |
| 随即恢复后迟到响应不能改记录/进度 | 恢复产生新一代 `run_id`，上一轮迟到响应仍无法通过代际校验，任务行更新还带 `run_id` CAS |
| 同任务并发恢复只启动一轮 | 每任务一把恢复锁，锁内检查在跑轮次，重复恢复返回 409 |
| 失败详情留待重试 | 详情失败只置 `failed` 并记 `last_error`，不阻塞其他详情 |
| 恢复/重启后保留已入库记录，只补未成功项 | 恢复轮只查询 `status != 'success'` 的记录补采；目录已抓完的页直接跳过 |
| 全部详情入库才算完成 | 仍有未成功项时任务置 `failed`；35 个唯一许可全部 `success` 才置 `completed` |
| 本地模拟公示站 | `mock_site/`：固定分页数据，支持可控详情延迟、首次失败 |
| 可启动的 Docker Compose | `docker compose up` 启动模拟站 + 采集 API，`--profile demo` 一键跑演示 |

## 2. 目录结构

```
.
├── collector/            # 采集 API 服务
│   ├── app.py            # FastAPI 路由（启动/暂停/恢复/进度/记录）
│   ├── engine.py         # 采集引擎：代际令牌、信号量、锁、恢复逻辑
│   ├── db.py             # aiosqlite 持久化层（WAL，复合主键去重）
│   ├── schemas.py        # 请求/响应模型
│   └── config.py         # 环境变量配置
├── mock_site/            # 本地模拟公示站
│   ├── app.py            # 目录/详情接口 + 延迟/首次失败控制接口
│   └── data.py           # 固定数据集（35 唯一许可 + 3 跨页重复）
├── demo/demo.py          # 端到端演示脚本（仅标准库）
├── tests/                # pytest 自测（14 个用例，真实 HTTP 回环）
├── Dockerfile.collector / Dockerfile.mock
├── docker-compose.yml
└── requirements.txt
```

## 3. 快速开始

### 方式 A：Docker Compose（推荐演示）

```bash
# 启动模拟公示站(8001) 与采集 API(8000)
docker compose up --build

# 另开终端，通过 API 入口一键演示（含暂停/恢复/首次失败补采）
docker compose --profile demo run --rm demo
```

采集 API：http://localhost:8000 ，交互式文档：http://localhost:8000/docs
模拟公示站：http://localhost:8001 ，交互式文档：http://localhost:8001/docs
SQLite 数据保存在 compose 命名卷 `collector-data` 中，容器重启不丢。

### 方式 B：本地直接运行

```bash
pip install -r requirements.txt

# 终端 1：模拟公示站
uvicorn mock_site.app:app --port 8001

# 终端 2：采集 API
MOCK_BASE_URL=http://127.0.0.1:8001 DB_PATH=./data/collector.db \
  uvicorn collector.app:app --port 8000

# 终端 3：演示
python demo/demo.py
```

### 手动调用示例

```bash
curl -X POST http://localhost:8000/api/tasks/start            # 启动
curl -s http://localhost:8000/api/tasks                      # 任务列表
curl -s http://localhost:8000/api/tasks/<id>/progress        # 进度
curl -X POST http://localhost:8000/api/tasks/<id>/pause      # 暂停
curl -X POST http://localhost:8000/api/tasks/<id>/resume     # 恢复
curl -s "http://localhost:8000/api/tasks/<id>/records?status=success&limit=50"
```

## 4. API 一览（采集服务，端口 8000）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/tasks/start` | 创建并启动采集任务，body 可空或 `{"target_url": "..."}` |
| POST | `/api/tasks/{id}/pause` | 暂停；响应返回后旧轮次写入全部被拒 |
| POST | `/api/tasks/{id}/resume` | 恢复，只补采未成功项；运行中重复调用返回 409 |
| GET | `/api/tasks/{id}/progress` | 状态、成功/失败/待采计数、在途数、目录页数 |
| GET | `/api/tasks/{id}/records` | 记录查询，支持 `status=pending|success|failed`、`limit`、`offset` |
| GET | `/api/tasks` | 全部任务 |
| GET | `/health` | 健康检查 |

任务状态机：`running → paused/failed/completed/interrupted`，其中 `paused/failed/interrupted` 均可 `resume`。

## 5. 模拟公示站控制接口（端口 8001）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/permits?page=N` | 固定分页目录（4 页，38 条目） |
| GET | `/api/permits/details/{detail_id}` | 许可详情 |
| POST | `/control/delay?seconds=0.4` | 详情接口统一延迟，制造在途请求 |
| POST | `/control/first-fail?detail_id=DET-005` | 该详情首次访问返回 500，之后自动恢复 |
| POST | `/control/reset` | 清空控制项与统计 |
| GET | `/control/state` | 查看请求计数、峰值在途数、失败记录 |

## 6. 关键设计：暂停后为什么写不进去

1. **代际令牌 `run_id`**：每次启动/恢复/暂停都生成新 `run_id` 写入 `tasks` 表。
   采集协程任何写库动作前都要通过 `_is_current(task_id, run_id)` 校验；
   暂停会立即换代并摘除控制块，旧轮次校验恒为 False。
2. **写锁 + CAS**：所有 SQLite 写入在全局异步写锁内“先校验再写入”，
   任务行更新附带 `WHERE run_id = ?` 条件更新，双重防止迟到响应改进度。
3. **信号量限并发**：每轮独立 `Semaphore(2)`，在途请求任何时刻不超过 2，
   模拟站 `/control/state` 的 `max_inflight` 可独立佐证。
4. **持久化与补采**：目录页、记录状态即时 commit；恢复时跳过已完成目录页，
   仅拉取 `pending/failed` 记录的详情。进程重启时遗留的 `running` 任务
   在启动阶段标记为 `interrupted`，等待人工恢复。

## 7. 自测

```bash
pip install -r requirements-dev.txt
pytest -q
```

覆盖：完整采集与字段、跨页去重、在途上限、暂停后零迟到写入、随即恢复隔离、
首次失败补采、并发恢复 409、进程重启恢复、running 中断标记、多任务同库、API 404/409。

## 8. 已知限制

- 采集目标按任务固定，目录协议与本模拟站一致（`/api/permits` + `/api/permits/details/{id}`）；
  接入真实站点需要在引擎内适配分页与详情字段映射。
- 单进程内通过异步锁保证并发语义；多副本水平部署需要把锁与代际状态外移（如 Redis/DB 行锁）。
- 本环境未安装 Docker，compose 配置仅做了静态校验，未实际构建镜像。
