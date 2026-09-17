# 多级配额裁决 API（Multi-Level Quota Decider）

使用 **Python 3.11 + FastAPI + SQLite** 实现的本地多级配额裁决服务。
配额策略可分别在 **全局（global）/ 租户（tenant）/ 路由（route）** 三级配置，
每级独立设置桶容量（capacity）与每秒令牌补充率（refill_rate）。

## 核心语义

- **惰性令牌桶**：桶只在被裁决/撤销命中时，按距上次更新经过的时间补充令牌，
  补充量 = `elapsed × refill_rate`，且余额永远不超过桶容量。
- **整组原子裁决**：一次请求命中的所有桶（全局 → 租户 → 路由，缺失的层不限制），
  在**同一个 `BEGIN IMMEDIATE` 事务**、同一个时间戳下完成「补充 → 判足 → 扣减」。
  - 所有桶都足够：全部同时扣减 `cost`，返回 `allowed=true`；
  - 任一桶不足：**一个令牌都不扣**（时间应得的补充仍会落库），返回 `allowed=false`。
- **整组最早可重试时间**：拒绝时，对每个不足桶计算就绪等待
  `(cost - balance) / refill_rate`，取**最大值**作为 `retry_at` / `retry_after_seconds`。
  若 `refill_rate=0` 或 `cost` 超过桶容量（永远不可能满足），返回
  `retry_possible=false`、`retry_at=null`。
- **幂等重放**：相同 `request_id` 且裁决载荷（tenant_id / route_id / cost）一致时，
  直接返回原裁决结果（`replayed=true`），不重复扣减；载荷冲突返回 **409**。
  通过与拒绝的裁决都会存档。
- **撤销**：可撤销已通过的裁决并向当时命中的桶补回令牌，重复撤销保持幂等；
  补回时同样先补充再退还，最终余额**不超过桶容量**。
- **低余额事件**：策略可配置 `low_balance_threshold`（`null` 关闭，否则须在
  `0` 到 `capacity` 之间）。一次**获准**裁决若令某命中桶的余额
  **从阈值之上降至不高于阈值**，会在扣减的同一事务内为该桶写入唯一一条
  `low_balance` 事件（递增 `id`、桶键、扣减前后余额、阈值、`request_id`、时间）。
  拒绝与幂等重放不产生事件；余额已在阈值之下的继续扣减也不算越线。
  当时间补充、撤销或策略更新（如调低阈值）使余额重新高于阈值后，
  后续再次越线会产生**新**事件。事件通过 `GET /events` 按 `id` 稳定递增分页读取。
- **并发不超发**：SQLite 处于 WAL 模式，写事务一律 `BEGIN IMMEDIATE` 立即加写锁，
  配合 `busy_timeout=30s` 让并发写者排队，读-改-写序列天然串行化。
  单进程多线程、多进程（多个实例共享同一数据库文件）下均不会超发。

## 一条命令启动（需要 Docker + Compose）

```bash
docker compose up --build
# 后台运行：
docker compose up --build -d
```

服务监听 **8000 端口**：

- API 基址：`http://localhost:8000`
- 健康检查：`GET http://localhost:8000/health`
- 交互式文档（Swagger UI）：`http://localhost:8000/docs`
- OpenAPI Schema：`http://localhost:8000/openapi.json`

SQLite 数据文件保存在命名卷 `quota-data`（容器内 `/data/quota.db`），
`docker compose down` 不会删除；如需清空：`docker compose down -v`。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET`  | `/health` | 健康检查 |
| `GET`  | `/policies` | 列出全部策略 |
| `PUT`  | `/policies` | 创建/更新一层策略（按 scope+tenant_id+route_id 幂等 upsert） |
| `GET`  | `/buckets` | 查看所有已创建桶补充后的实时余额（只读，不落库） |
| `POST` | `/decide` | 提交一次多级裁决 |
| `POST` | `/decide/{request_id}/revoke` | 撤销一次已通过的裁决（重复撤销幂等） |
| `GET`  | `/events` | 按 id 递增分页查询 low_balance 事件（`after_id` / `limit`） |

策略对象：

```json
{
  "scope": "global | tenant | route",
  "tenant_id": "acme（tenant/route 必填）",
  "route_id": "/v1/search（route 必填，且必须带 tenant_id）",
  "capacity": 10,
  "refill_rate": 0.5,
  "low_balance_threshold": 2
}
```

> `low_balance_threshold` 可省略或为 `null`：关闭该层的低余额事件；
> 否则必须在 `0` 到 `capacity` 之间（含边界），超出范围返回 `422`。
> `PUT` 为全量替换：更新策略时不传该字段即视为关闭。

事件对象（`GET /events` 的返回元素）：

```json
{
  "id": 1,
  "scope": "tenant",
  "tenant_id": "acme",
  "route_id": null,
  "balance_before": 7.0,
  "balance_after": 4.0,
  "threshold": 4.0,
  "request_id": "req-1",
  "created_at": "2026-09-17T12:56:09.556564Z"
}
```

| 字段 | 含义 |
| --- | --- |
| `id` | 单调递增的事件 id，用作分页游标 |
| `scope` / `tenant_id` / `route_id` | 触发事件的桶键 |
| `balance_before` / `balance_after` | 本次裁决扣减前（时间补充后）/ 扣减后的余额 |
| `threshold` | 触发本事件时策略上的低余额阈值 |
| `request_id` | 触发本事件的裁决幂等键 |
| `created_at` | 事件时间（ISO 8601 UTC） |

裁决请求：

```json
{
  "request_id": "req-001",
  "tenant_id": "acme",
  "route_id": "/v1/search",
  "cost": 3
}
```

> `tenant_id`、`route_id` 均可省略；省略即不命中对应层级。
> `cost` 对所有命中桶统一扣减同一数量。

## 示例请求

### 1. 配置三级策略

```bash
# 全局：容量 10，0.5 令牌/秒
curl -s -X PUT http://localhost:8000/policies \
  -H 'Content-Type: application/json' \
  -d '{"scope":"global","capacity":10,"refill_rate":0.5}'

# 租户 acme：容量 6，1 令牌/秒
curl -s -X PUT http://localhost:8000/policies \
  -H 'Content-Type: application/json' \
  -d '{"scope":"tenant","tenant_id":"acme","capacity":6,"refill_rate":1}'

# 路由（acme 的 /v1/search）：容量 4，2 令牌/秒
curl -s -X PUT http://localhost:8000/policies \
  -H 'Content-Type: application/json' \
  -d '{"scope":"route","tenant_id":"acme","route_id":"/v1/search","capacity":4,"refill_rate":2}'
```

### 2. 裁决（通过，三级桶同时扣 3）

```bash
curl -s -X POST http://localhost:8000/decide \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-1","tenant_id":"acme","route_id":"/v1/search","cost":3}'
```

响应中 `buckets` 返回每个命中桶的容量、补充率和裁决后余额（7 / 3 / 1）：

```json
{
  "request_id": "req-1",
  "allowed": true,
  "cost": 3.0,
  "tenant_id": "acme",
  "route_id": "/v1/search",
  "decided_at": "2026-09-17T12:56:09.556564Z",
  "retry_at": null,
  "retry_after_seconds": null,
  "retry_possible": true,
  "replayed": false,
  "revoked": false,
  "revoked_at": null,
  "buckets": [
    {"scope": "global", "tenant_id": null, "route_id": null, "capacity": 10.0, "refill_rate": 0.5, "balance": 7.0},
    {"scope": "tenant", "tenant_id": "acme", "route_id": null, "capacity": 6.0, "refill_rate": 1.0, "balance": 3.0},
    {"scope": "route",  "tenant_id": "acme", "route_id": "/v1/search", "capacity": 4.0, "refill_rate": 2.0, "balance": 1.0}
  ]
}
```

### 3. 裁决（路由桶不足 → 整组拒绝，全部不扣）

```bash
curl -s -X POST http://localhost:8000/decide \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-2","tenant_id":"acme","route_id":"/v1/search","cost":2}'
```

```json
{
  "request_id": "req-2",
  "allowed": false,
  "retry_at": "2026-09-17T12:56:10.056564Z",
  "retry_after_seconds": 0.48,
  "retry_possible": true,
  "buckets": [ {"scope": "global", "balance": 7.0}, {"scope": "tenant", "balance": 3.0}, {"scope": "route", "balance": 1.0} ]
}
```

### 4. 幂等重放与 409 冲突

```bash
# 相同 request_id + 相同载荷：重放原结果（replayed=true），不再扣令牌
curl -s -X POST http://localhost:8000/decide \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-1","tenant_id":"acme","route_id":"/v1/search","cost":3}'

# 相同 request_id 但载荷不同（如 cost 改变）：HTTP 409
curl -i -X POST http://localhost:8000/decide \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-1","tenant_id":"acme","route_id":"/v1/search","cost":4}'
```

### 5. 撤销与重复撤销

```bash
# 撤销已通过的裁决，向当时命中的桶补回令牌（不超过容量）
curl -s -X POST http://localhost:8000/decide/req-1/revoke

# 再次撤销：幂等，返回同一撤销结果（replayed=true）
curl -s -X POST http://localhost:8000/decide/req-1/revoke
```

对被拒绝的裁决撤销返回 409；对不存在的 `request_id` 撤销返回 404。

### 6. 查看桶余额

```bash
curl -s http://localhost:8000/buckets
```

### 7. 低余额事件：配置阈值、触发与分页查询

```bash
# 给租户 acme 配置阈值 2：余额从 2 以上降到 2 或以下时产生事件
curl -s -X PUT http://localhost:8000/policies \
  -H 'Content-Type: application/json' \
  -d '{"scope":"tenant","tenant_id":"acme","capacity":6,"refill_rate":1,"low_balance_threshold":2}'

# 裁决通过，租户桶 6 -> 3：仍在阈值之上，无事件
curl -s -X POST http://localhost:8000/decide \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-10","tenant_id":"acme","cost":3}'

# 再次裁决，租户桶 3 -> 1：越过阈值，写入一条 low_balance 事件
curl -s -X POST http://localhost:8000/decide \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-11","tenant_id":"acme","cost":2}'

# 查询事件（默认按 id 升序返回最多 100 条）
curl -s http://localhost:8000/events

# 稳定分页：limit 控制页大小，after_id 传上一页最后一条的 id
curl -s 'http://localhost:8000/events?limit=10'
curl -s 'http://localhost:8000/events?after_id=1&limit=10'
```

事件响应示例：

```json
[
  {
    "id": 1,
    "scope": "tenant",
    "tenant_id": "acme",
    "route_id": null,
    "balance_before": 3.0,
    "balance_after": 1.0,
    "threshold": 2.0,
    "request_id": "req-11",
    "created_at": "2026-09-17T12:56:12.000000Z"
  }
]
```

## 行为约定与边界

- 桶首次被命中时按当前容量**满额**创建。
- 更新策略容量时，已有桶先按旧率补充到当前时刻，再截断到新容量
  （缩小容量不会让余额超过新容量）。
- 低余额事件只在**获准**裁决的扣减事务内产生：拒绝、幂等重放、
  撤销与策略更新本身都不写事件；一次裁决中每个越线桶各写一条。
- 阈值判定为「扣减前余额 > 阈值 ≥ 扣减后余额」；余额本就在阈值之下的
  扣减不会重复触发，待余额重新高于阈值后才会再次武装。
- 若一次裁决没有命中任何已配置的层级，返回 `404`（提示先配置策略）。
- `cost ≤ 0` 返回 `422`；`route_id` 必须与 `tenant_id` 一起提供，否则 `422`；
  `low_balance_threshold` 超出 `[0, capacity]` 返回 `422`。
- 裁决只保证「整组原子扣减」与最终一致；重试时间是服务端按补充率给出的建议值，
  期间若有其他裁决/撤销发生，实际结果仍以再次裁决为准。

## 本地开发（不用 Docker）

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
QUOTA_DB_PATH=./quota.db .venv/bin/uvicorn app.main:app --reload --port 8000
```

运行测试（另需 `pip install pytest httpx`）：

```bash
.venv/bin/pip install pytest httpx
.venv/bin/python -m pytest -q
```

测试覆盖：三级原子扣减/整组拒绝不扣、按时间补充、整组重试时间、
幂等重放、载荷冲突 409、撤销补回与幂等、补回不超容量、容量更新截断、
低余额事件的越线触发/拒绝与重放不触发/补充、撤销、阈值调低后重新武装、
多级同时越线各产一条、events 稳定分页、老库迁移补列，
以及 20+ 线程并发与双进程共享同一数据库文件均不超发。

## 项目结构

```
app/
  database.py   # SQLite 连接、WAL/busy_timeout、BEGIN IMMEDIATE 事务、建表与老库迁移
  quota.py      # 策略 upsert、惰性补充、整组裁决、幂等存档、撤销与低余额事件
  schemas.py    # Pydantic 请求/响应模型
  main.py       # FastAPI 路由
tests/
  test_api.py   # 行为与并发测试
Dockerfile
docker-compose.yml
```
