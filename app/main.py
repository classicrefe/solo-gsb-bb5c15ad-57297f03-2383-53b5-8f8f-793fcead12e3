"""FastAPI 入口：多级配额裁决服务。

启动后默认监听 8000 端口，交互式文档位于 /docs。
每个写请求使用独立连接 + BEGIN IMMEDIATE 事务，天然串行化，
并发裁决不会超发（见 app/database.py 与 app/quota.py）。
"""

from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager
from typing import Iterator

from fastapi import Depends, FastAPI, Query

from . import quota
from .database import connect, init_db, write_tx
from .schemas import (
    BucketOut,
    DecideIn,
    DecisionOut,
    LowBalanceEventOut,
    PolicyIn,
    PolicyOut,
    RevokeOut,
)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="多级配额裁决 API",
    description=(
        "全局 / 租户 / 路由三级令牌桶配额裁决。"
        "一次裁决在同一事务中补充并扣减所有命中桶，全部满足才扣减；"
        "request_id 幂等重放；支持撤销补回；"
        "策略可配置 low_balance_threshold，余额越线时产生 low_balance 事件。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)


def get_conn() -> Iterator[sqlite3.Connection]:
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


# ---------------------------------------------------------------- 策略


@app.get("/policies", response_model=list[PolicyOut], tags=["策略"])
def get_policies(conn: sqlite3.Connection = Depends(get_conn)) -> list[dict]:
    return quota.list_policies(conn)


@app.put("/policies", response_model=PolicyOut, tags=["策略"])
def put_policy(
    body: PolicyIn, conn: sqlite3.Connection = Depends(get_conn)
) -> dict:
    with write_tx(conn):
        return quota.upsert_policy(
            conn,
            scope=body.scope,
            tenant_id=body.tenant_id,
            route_id=body.route_id,
            capacity=body.capacity,
            refill_rate=body.refill_rate,
            low_balance_threshold=body.low_balance_threshold,
        )


# ---------------------------------------------------------------- 桶观察


@app.get("/buckets", response_model=list[BucketOut], tags=["桶状态"])
def get_buckets(conn: sqlite3.Connection = Depends(get_conn)) -> list[dict]:
    """查看所有已创建桶的当前（补充后）余额，便于排障。只读，不落库。"""
    return quota.list_buckets(conn)


# ---------------------------------------------------------------- 事件


@app.get("/events", response_model=list[LowBalanceEventOut], tags=["事件"])
def get_events(
    after_id: int = Query(default=0, ge=0, description="游标：只返回 id 大于它的事件"),
    limit: int = Query(default=100, ge=1, le=1000, description="每页最多返回条数"),
    conn: sqlite3.Connection = Depends(get_conn),
) -> list[dict]:
    """按 id 稳定递增分页返回 low_balance 事件。

    用法：先取 `GET /events?limit=N`，然后以最后一条的 id 作为
    `after_id` 继续翻页，直到返回空列表。
    """
    return quota.list_events(conn, after_id=after_id, limit=limit)


# ---------------------------------------------------------------- 裁决 / 撤销


@app.post(
    "/decide",
    response_model=DecisionOut,
    tags=["裁决"],
    responses={409: {"description": "request_id 已存在但载荷冲突"}},
)
def decide(body: DecideIn, conn: sqlite3.Connection = Depends(get_conn)) -> dict:
    with write_tx(conn):
        result, _replayed = quota.decide(
            conn,
            request_id=body.request_id,
            tenant_id=body.tenant_id,
            route_id=body.route_id,
            cost=body.cost,
        )
        return result


@app.post(
    "/decide/{request_id}/revoke",
    response_model=RevokeOut,
    tags=["裁决"],
    responses={
        404: {"description": "裁决不存在"},
        409: {"description": "裁决本身被拒绝，无需撤销"},
    },
)
def revoke_decision(
    request_id: str, conn: sqlite3.Connection = Depends(get_conn)
) -> dict:
    with write_tx(conn):
        result, _duplicate = quota.revoke(conn, request_id)
        return result
