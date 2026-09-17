"""配额裁决核心逻辑：多级令牌桶、幂等存档与撤销。

所有公共写操作都必须在 database.write_tx（BEGIN IMMEDIATE）事务中调用，
保证「补充 -> 判足 -> 扣减」这一读-改-写序列的原子性与隔离性。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import HTTPException

# 各策略层的空标识统一用空串存储（SQLite STRICT 主键列不允许 NULL 参与唯一比较）
EMPTY = ""


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    """统一的 ISO 8601 UTC 文本，例如 2026-09-17T12:00:00.123456Z。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_iso(text: str) -> datetime:
    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def payload_hash(payload: dict[str, Any]) -> str:
    """对除 request_id 外的裁决载荷做规范序列化后取 SHA-256。"""
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------- 策略管理


def list_policies(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT scope, tenant_id, route_id, capacity, refill_rate, "
        "low_balance_threshold, updated_at "
        "FROM policies ORDER BY scope, tenant_id, route_id"
    ).fetchall()
    return [_policy_out(r) for r in rows]


def upsert_policy(
    conn: sqlite3.Connection,
    *,
    scope: str,
    tenant_id: Optional[str],
    route_id: Optional[str],
    capacity: float,
    refill_rate: float,
    low_balance_threshold: Optional[float] = None,
) -> dict[str, Any]:
    """创建或更新一层策略。

    更新容量时，已存在的桶先按旧策略补充到当前时刻，再截断到新容量，
    保证余额永远不超过当前容量。
    low_balance_threshold 为 None 时关闭该层的低余额事件；否则须在
    [0, capacity] 内。PUT 为全量替换：不传即视为关闭。
    """
    tenant = tenant_id or EMPTY
    route = route_id or EMPTY
    if scope == "global":
        tenant, route = EMPTY, EMPTY
    elif scope == "tenant":
        route = EMPTY
        if not tenant:
            raise HTTPException(422, "tenant 作用域必须提供 tenant_id")
    elif scope == "route":
        if not tenant or not route:
            raise HTTPException(422, "route 作用域必须同时提供 tenant_id 与 route_id")
    else:
        raise HTTPException(422, f"非法 scope: {scope!r}")
    if capacity < 0 or refill_rate < 0:
        raise HTTPException(422, "capacity 与 refill_rate 必须非负")
    if low_balance_threshold is not None and not (
        0.0 <= low_balance_threshold <= capacity
    ):
        raise HTTPException(
            422, "low_balance_threshold 必须为 null（关闭）或在 0 到 capacity 之间"
        )

    ts = now_utc()
    old = conn.execute(
        "SELECT capacity, refill_rate FROM policies "
        "WHERE scope=? AND tenant_id=? AND route_id=?",
        (scope, tenant, route),
    ).fetchone()

    bucket = conn.execute(
        "SELECT balance, updated_at FROM buckets "
        "WHERE scope=? AND tenant_id=? AND route_id=?",
        (scope, tenant, route),
    ).fetchone()
    if bucket is not None:
        # 用旧策略（若存在）先把应补充的令牌补上，再按新容量截断
        cap_old = old["capacity"] if old else capacity
        rate_old = old["refill_rate"] if old else refill_rate
        balance = _refill(bucket["balance"], bucket["updated_at"], cap_old, rate_old, ts)
        balance = min(balance, capacity)
        conn.execute(
            "UPDATE buckets SET balance=?, updated_at=? "
            "WHERE scope=? AND tenant_id=? AND route_id=?",
            (balance, iso(ts), scope, tenant, route),
        )

    conn.execute(
        """
        INSERT INTO policies
            (scope, tenant_id, route_id, capacity, refill_rate,
             low_balance_threshold, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (scope, tenant_id, route_id) DO UPDATE SET
            capacity=excluded.capacity,
            refill_rate=excluded.refill_rate,
            low_balance_threshold=excluded.low_balance_threshold,
            updated_at=excluded.updated_at
        """,
        (
            scope, tenant, route, float(capacity), float(refill_rate),
            None if low_balance_threshold is None else float(low_balance_threshold),
            iso(ts),
        ),
    )
    row = conn.execute(
        "SELECT scope, tenant_id, route_id, capacity, refill_rate, "
        "low_balance_threshold, updated_at "
        "FROM policies WHERE scope=? AND tenant_id=? AND route_id=?",
        (scope, tenant, route),
    ).fetchone()
    return _policy_out(row)


def _policy_out(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "scope": row["scope"],
        "tenant_id": row["tenant_id"] or None,
        "route_id": row["route_id"] or None,
        "capacity": row["capacity"],
        "refill_rate": row["refill_rate"],
        "low_balance_threshold": row["low_balance_threshold"],
        "updated_at": row["updated_at"],
    }


# ---------------------------------------------------------------- 令牌桶


def _refill(
    balance: float,
    updated_at: str,
    capacity: float,
    refill_rate: float,
    now: datetime,
) -> float:
    elapsed = max(0.0, (now - parse_iso(updated_at)).total_seconds())
    return min(float(capacity), float(balance) + elapsed * float(refill_rate))


def _bucket_out(
    scope: str,
    tenant: str,
    route: str,
    capacity: float,
    refill_rate: float,
    balance: float,
) -> dict[str, Any]:
    return {
        "scope": scope,
        "tenant_id": tenant or None,
        "route_id": route or None,
        "capacity": float(capacity),
        "refill_rate": float(refill_rate),
        "balance": float(balance),
    }


def list_buckets(conn: sqlite3.Connection, *, live: bool = True) -> list[dict[str, Any]]:
    """列出所有已创建的桶；live=True 时展示补充到当前时刻的余额（不落库）。"""
    rows = conn.execute(
        """
        SELECT b.scope, b.tenant_id, b.route_id, b.balance, b.updated_at,
               p.capacity, p.refill_rate
        FROM buckets b JOIN policies p
          ON p.scope=b.scope AND p.tenant_id=b.tenant_id AND p.route_id=b.route_id
        ORDER BY b.scope, b.tenant_id, b.route_id
        """
    ).fetchall()
    now = now_utc()
    out: list[dict[str, Any]] = []
    for r in rows:
        balance = (
            _refill(r["balance"], r["updated_at"], r["capacity"], r["refill_rate"], now)
            if live
            else r["balance"]
        )
        out.append(
            _bucket_out(
                r["scope"], r["tenant_id"], r["route_id"],
                r["capacity"], r["refill_rate"], balance,
            )
        )
    return out


# ---------------------------------------------------------------- 裁决


def _maybe_emit_low_balance(
    conn: sqlite3.Connection,
    state: dict[str, Any],
    *,
    before: float,
    after: float,
    request_id: str,
    now: datetime,
) -> None:
    """获准扣减令桶「从阈值之上降至不高于阈值」时，在扣减事务内写入唯一事件。

    阈值关闭（None）或本次未越线时不写。余额已在阈值之下的继续扣减不算
    越线；只有当补充/撤销/策略更新把余额重新抬到阈值之上后，再次越线才
    会产生新事件。
    """
    threshold = state["low_balance_threshold"]
    if threshold is None:
        return
    # after 是浮点减法结果，给「不高于阈值」一点容差，避免恰好等于阈值时漏报
    if before > threshold and after <= threshold + 1e-12:
        conn.execute(
            """
            INSERT INTO low_balance_events
                (scope, tenant_id, route_id, balance_before, balance_after,
                 threshold, request_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                state["scope"], state["tenant"], state["route"],
                float(before), float(after), float(threshold),
                request_id, iso(now),
            ),
        )


def decide(
    conn: sqlite3.Connection,
    *,
    request_id: str,
    tenant_id: Optional[str],
    route_id: Optional[str],
    cost: float,
) -> tuple[dict[str, Any], bool]:
    """对一次请求做多级裁决，返回 (结果, 是否为重放)。

    命中的每个桶在同一事务、同一时刻 now 下：
      1. 按距上次更新经过的时间补充令牌（不超过容量）；
      2. 全部桶余额都足够才统一扣减；任一不足则一个都不扣。
    """
    if cost <= 0:
        raise HTTPException(422, "cost 必须为正数")
    if route_id and not tenant_id:
        raise HTTPException(422, "提供 route_id 时必须同时提供 tenant_id")

    # 1) 幂等：request_id 已存在时，载荷一致则重放，冲突则 409
    existing = conn.execute(
        "SELECT payload_hash, result_json FROM decisions WHERE request_id=?",
        (request_id,),
    ).fetchone()
    payload = {"tenant_id": tenant_id or None, "route_id": route_id or None, "cost": cost}
    digest = payload_hash(payload)
    payload_json = json.dumps(payload, ensure_ascii=False)
    if existing is not None:
        if existing["payload_hash"] != digest:
            raise HTTPException(
                409,
                f"request_id={request_id!r} 已存在但裁决载荷不一致，拒绝重放",
            )
        result = json.loads(existing["result_json"])
        result["replayed"] = True
        return result, True

    now = now_utc()

    # 2) 解析本次请求命中的策略层（全局 -> 租户 -> 路由），缺省层不做限制
    levels: list[tuple[str, str, str, sqlite3.Row]] = []
    g = conn.execute(
        "SELECT capacity, refill_rate, low_balance_threshold FROM policies "
        "WHERE scope='global' AND tenant_id='' AND route_id=''"
    ).fetchone()
    if g is not None:
        levels.append(("global", EMPTY, EMPTY, g))
    if tenant_id:
        t = conn.execute(
            "SELECT capacity, refill_rate, low_balance_threshold FROM policies "
            "WHERE scope='tenant' AND tenant_id=? AND route_id=''",
            (tenant_id,),
        ).fetchone()
        if t is not None:
            levels.append(("tenant", tenant_id, EMPTY, t))
        if route_id:
            r = conn.execute(
                "SELECT capacity, refill_rate, low_balance_threshold FROM policies "
                "WHERE scope='route' AND tenant_id=? AND route_id=?",
                (tenant_id, route_id),
            ).fetchone()
            if r is not None:
                levels.append(("route", tenant_id, route_id, r))

    if not levels:
        raise HTTPException(404, "没有任何命中的配额策略（请先配置至少一层策略）")

    # 3) 取桶（惰性创建，新桶满额），并在同一 now 下补充
    states: list[dict[str, Any]] = []
    for scope, tenant, route, pol in levels:
        row = conn.execute(
            "SELECT balance, updated_at FROM buckets "
            "WHERE scope=? AND tenant_id=? AND route_id=?",
            (scope, tenant, route),
        ).fetchone()
        if row is None:
            balance = float(pol["capacity"])
            conn.execute(
                "INSERT INTO buckets (scope, tenant_id, route_id, balance, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (scope, tenant, route, balance, iso(now)),
            )
        else:
            balance = _refill(
                row["balance"], row["updated_at"],
                pol["capacity"], pol["refill_rate"], now,
            )
        states.append(
            {
                "scope": scope, "tenant": tenant, "route": route,
                "capacity": float(pol["capacity"]),
                "refill_rate": float(pol["refill_rate"]),
                "low_balance_threshold": pol["low_balance_threshold"],
                "balance": balance,
            }
        )

    allowed = all(s["balance"] + 1e-12 >= cost for s in states)

    # 4) 不足：整组一个都不扣（补充本身会落库，这是时间应得的令牌）；
    #    充足：所有命中桶在同一事务中一起扣减
    if allowed:
        for s in states:
            before = s["balance"]
            after = before - float(cost)
            s["balance"] = after
            conn.execute(
                "UPDATE buckets SET balance=?, updated_at=? "
                "WHERE scope=? AND tenant_id=? AND route_id=?",
                (after, iso(now), s["scope"], s["tenant"], s["route"]),
            )
            _maybe_emit_low_balance(
                conn, s, before=before, after=after,
                request_id=request_id, now=now,
            )
        retry_at, retry_after, retry_possible = None, None, True
    else:
        # 整组最早可重试时刻 = 各不足桶就绪耗时的最大值
        waits: list[float] = []
        retry_possible = True
        for s in states:
            if s["balance"] + 1e-12 >= cost:
                continue
            deficit = float(cost) - s["balance"]
            rate = s["refill_rate"]
            # rate=0 永不补充，或 cost 超过桶容量 -> 无论等多久都不可能通过
            if rate <= 0 or float(cost) > s["capacity"] + 1e-12:
                retry_possible = False
                break
            waits.append(deficit / rate)
        if retry_possible:
            wait = max(waits)
            retry_dt = now + timedelta(seconds=wait)
            retry_at, retry_after = iso(retry_dt), max(0.0, wait)
        else:
            retry_at, retry_after = None, None
        # 拒绝不扣减，但补充后的余额（时间应得的令牌）一律落库
        for s in states:
            conn.execute(
                "UPDATE buckets SET balance=?, updated_at=? "
                "WHERE scope=? AND tenant_id=? AND route_id=?",
                (s["balance"], iso(now), s["scope"], s["tenant"], s["route"]),
            )

    result = {
        "request_id": request_id,
        "allowed": allowed,
        "cost": float(cost),
        "tenant_id": tenant_id or None,
        "route_id": route_id or None,
        "decided_at": iso(now),
        "retry_at": retry_at,
        "retry_after_seconds": retry_after,
        "retry_possible": retry_possible,
        "replayed": False,
        "revoked": False,
        "buckets": [
            _bucket_out(
                s["scope"], s["tenant"], s["route"],
                s["capacity"], s["refill_rate"], s["balance"],
            )
            for s in states
        ],
    }

    # 5) 存档（通过与拒绝都存），保证 request_id 可重放
    conn.execute(
        """
        INSERT INTO decisions
            (request_id, payload_hash, payload_json, allowed, cost, decided_at,
             retry_at, retry_after_secs, result_json, revoked, revoked_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)
        """,
        (
            request_id, digest, payload_json, int(allowed), float(cost), iso(now),
            retry_at, retry_after, json.dumps(result, ensure_ascii=False),
        ),
    )
    return result, False


# ---------------------------------------------------------------- 撤销


def revoke(conn: sqlite3.Connection, request_id: str) -> tuple[dict[str, Any], bool]:
    """撤销一次已通过的裁决并补回令牌，返回 (结果, 是否为重复撤销)。

    重复撤销幂等；被拒绝/不存在的 request_id 分别返回 409 / 404。
    每个桶先按经过时间补充，再加回 cost，最后截断到容量。
    """
    row = conn.execute(
        "SELECT allowed, cost, payload_json, result_json, revoked FROM decisions "
        "WHERE request_id=?",
        (request_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(404, f"request_id={request_id!r} 不存在")
    if not row["allowed"]:
        raise HTTPException(409, f"request_id={request_id!r} 是被拒绝的裁决，无需撤销")
    if row["revoked"]:
        result = json.loads(row["result_json"])
        result["replayed"] = True
        return result, True

    payload = json.loads(row["payload_json"])
    cost = float(row["cost"])
    now = now_utc()

    # 用原裁决载荷定位当时命中的桶；桶行在裁决通过时必然已创建
    keys: list[tuple[str, str, str]] = [("global", EMPTY, EMPTY)]
    if payload.get("tenant_id"):
        keys.append(("tenant", payload["tenant_id"], EMPTY))
        if payload.get("route_id"):
            keys.append(("route", payload["tenant_id"], payload["route_id"]))

    refunded_buckets: list[dict[str, Any]] = []
    for scope, tenant, route in keys:
        pol = conn.execute(
            "SELECT capacity, refill_rate FROM policies "
            "WHERE scope=? AND tenant_id=? AND route_id=?",
            (scope, tenant, route),
        ).fetchone()
        if pol is None:
            continue  # 策略已被移除（当前版本不支持删除，仅作防御）
        b = conn.execute(
            "SELECT balance, updated_at FROM buckets "
            "WHERE scope=? AND tenant_id=? AND route_id=?",
            (scope, tenant, route),
        ).fetchone()
        if b is None:
            continue
        # 先补充时间应得的令牌，再补回本次扣减，并以容量为上限
        balance = _refill(
            b["balance"], b["updated_at"],
            pol["capacity"], pol["refill_rate"], now,
        )
        balance = min(float(pol["capacity"]), balance + cost)
        conn.execute(
            "UPDATE buckets SET balance=?, updated_at=? "
            "WHERE scope=? AND tenant_id=? AND route_id=?",
            (balance, iso(now), scope, tenant, route),
        )
        refunded_buckets.append(
            _bucket_out(
                scope, tenant, route,
                pol["capacity"], pol["refill_rate"], balance,
            )
        )

    revoked_at = iso(now)
    result = json.loads(row["result_json"])
    result["revoked"] = True
    result["revoked_at"] = revoked_at
    result["replayed"] = False
    result["buckets"] = refunded_buckets

    conn.execute(
        "UPDATE decisions SET revoked=1, revoked_at=?, result_json=? WHERE request_id=?",
        (revoked_at, json.dumps(result, ensure_ascii=False), request_id),
    )
    return result, False


# ---------------------------------------------------------------- 事件


def list_events(
    conn: sqlite3.Connection, *, after_id: int = 0, limit: int = 100
) -> list[dict[str, Any]]:
    """按 id 递增返回低余额事件；after_id 为游标（不含），limit 为页大小。"""
    rows = conn.execute(
        "SELECT id, scope, tenant_id, route_id, balance_before, balance_after, "
        "threshold, request_id, created_at "
        "FROM low_balance_events WHERE id > ? ORDER BY id LIMIT ?",
        (after_id, limit),
    ).fetchall()
    return [
        {
            "id": r["id"],
            "scope": r["scope"],
            "tenant_id": r["tenant_id"] or None,
            "route_id": r["route_id"] or None,
            "balance_before": r["balance_before"],
            "balance_after": r["balance_after"],
            "threshold": r["threshold"],
            "request_id": r["request_id"],
            "created_at": r["created_at"],
        }
        for r in rows
    ]
