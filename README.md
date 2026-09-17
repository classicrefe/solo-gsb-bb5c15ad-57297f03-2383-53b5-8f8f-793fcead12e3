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
- **策略定时切换**：通过 `POST /policy-changes` 为**已有策略**登记一条未来 UTC
  时刻（`effective_at`）生效的变更（新容量、新补充速率、新低余额阈值），以
  唯一 `change_id` 幂等标识。同一策略至多一条**待生效**变更；待生效期间
  `PUT /policies` 直接更新该策略会返回 `409`；生效前可用
  `DELETE /policy-changes/{change_id}` 取消（重复取消幂等）。
  **无需后台任务**：每次 `/decide`（含幂等重放）或撤销在同一
  `BEGIN IMMEDIATE` 事务内先套用所有命中且到期的变更，再执行原裁决——
  变更落库与本次裁决原子完成，并发请求只能观察到完整的旧状态或新状态。
  套用时：桶先按**旧策略**补充到 `effective_at`，余额再按
  `旧余额 / 旧容量 × 新容量` 等比缩放（**旧容量为 0 时余额为 0**），
  随后按**新速率**从 `effective_at` 补充到裁决时刻。
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
| `PUT`  | `/policies` | 创建/更新一层策略（按 scope+tenant_id+route_id 幂等 upsert；有待生效变更时 409） |
| `GET`  | `/policy-changes` | 列出全部策略定时变更（待生效/已生效/已取消） |
| `POST` | `/policy-changes` | 登记一条未来时刻生效的策略变更（同 change_id 重提幂等） |
| `DELETE` | `/policy-changes/{change_id}` | 生效前取消一条变更（重复取消幂等） |
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

### 8. 策略定时切换：登记、查询、到期自动套用与取消

```bash
# 先配置一层策略（变更只能挂在已有策略上）
curl -s -X PUT http://localhost:8000/policies \
  -H 'Content-Type: application/json' \
  -d '{"scope":"global","capacity":10,"refill_rate":1}'

# 计算一个未来 UTC 时刻（例：60 秒后，ISO 8601）
EFFECTIVE_AT=$(python3 -c \
  "from datetime import datetime,timedelta,timezone; \
print((datetime.now(timezone.utc)+timedelta(seconds=60)).isoformat().replace('+00:00','Z'))")

# 登记变更：到点后容量 20、2 令牌/秒、低余额阈值 4
curl -s -X POST http://localhost:8000/policy-changes \
  -H 'Content-Type: application/json' \
  -d "{
    \"change_id\": \"ch-20260917-001\",
    \"scope\": \"global\",
    \"capacity\": 20,
    \"refill_rate\": 2,
    \"low_balance_threshold\": 4,
    \"effective_at\": \"$EFFECTIVE_AT\"
  }"
```

成功返回 `201 Created`（相同 `change_id` + 相同载荷重提返回 `200`）：

```json
{
  "change_id": "ch-20260917-001",
  "scope": "global",
  "tenant_id": null,
  "route_id": null,
  "capacity": 20.0,
  "refill_rate": 2.0,
  "low_balance_threshold": 4.0,
  "effective_at": "2026-09-17T13:00:00.000000Z",
  "status": "pending",
  "created_at": "2026-09-17T12:59:00.000000Z",
  "applied_at": null,
  "cancelled_at": null
}
```

到期后**不需要任何后台操作**：下一次裁决（即使是旧 `request_id` 的幂等重放）
或撤销会在同一事务内自动套用，`GET /policy-changes` 中该记录变为
`status="applied"` 并带 `applied_at`。余额迁移示例：旧容量 10、速率 1、
到期时余额 6，则新容量 20 下余额先变为 `6/10×20 = 12`，再按 2 令牌/秒
从生效时刻补充到裁决时刻。

```bash
# 待生效期间直接 PUT 同一策略 -> 409（须先取消或等其生效）
curl -i -X PUT http://localhost:8000/policies \
  -H 'Content-Type: application/json' \
  -d '{"scope":"global","capacity":99,"refill_rate":1}'

# 同一策略登记第二条待生效变更 -> 409
curl -i -X POST http://localhost:8000/policy-changes \
  -H 'Content-Type: application/json' \
  -d '{"change_id":"ch-other","scope":"global","capacity":30,"refill_rate":1,"effective_at":"'$EFFECTIVE_AT'"}'

# 生效前取消（再调一次幂等）；已生效的变更取消返回 409
curl -s -X DELETE http://localhost:8000/policy-changes/ch-20260917-001

# 查看全部变更（含 applied / cancelled 历史，按登记顺序）
curl -s http://localhost:8000/policy-changes
```

> `effective_at` 必须是**未来**的 UTC ISO 8601 时间（如
> `2026-09-17T13:00:00Z`），过去时间返回 `422`；目标策略不存在返回 `404`。
> 阈值校验与 `PUT /policies` 一致：`null` 关闭，否则须在 `0` 到新 `capacity`
> 之间。`GET /buckets` 为只读排障视图：对已到期但尚未被写请求套用的变更，
> 会按相同规则投影出切换后的余额，但不会提前落库。

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
- 定时变更只能挂在**已有策略**上（否则 `404`），`effective_at` 必须是未来
  UTC 时间（否则 `422`）；同一策略至多一条待生效变更，重复登记或待生效期间
  直接 `PUT` 该策略均返回 `409`；已生效的变更不可取消（`409`），取消不存在的
  变更返回 `404`。到期套用是惰性的：没有任何裁决/撤销流量的策略即使过了
  `effective_at` 也保持原状，直到首次写请求在同一事务内完成切换。
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
策略定时变更的登记校验/幂等/单待生效约束/PUT 409/取消生命周期/
到期按比例缩放与新速率补充（旧容量 0、阈值切换、重放与撤销触发、
只读投影）/并发登记与并发裁决原子切换，
以及 20+ 线程并发与双进程共享同一数据库文件均不超发。

## 项目结构

```
app/
  database.py   # SQLite 连接、WAL/busy_timeout、BEGIN IMMEDIATE 事务、建表与老库迁移
  quota.py      # 策略 upsert、定时变更登记/取消/到期套用、惰性补充、整组裁决、幂等存档、撤销与低余额事件
  schemas.py    # Pydantic 请求/响应模型
  main.py       # FastAPI 路由
tests/
  test_api.py   # 行为与并发测试
Dockerfile
docker-compose.yml
```
