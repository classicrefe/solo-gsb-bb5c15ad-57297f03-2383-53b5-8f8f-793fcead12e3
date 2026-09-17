"""配额裁决核心逻辑：多级令牌桶、幂等存档、撤销与策略定时切换。

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


def _normalize_policy_key(
    scope: str, tenant_id: Optional[str], route_id: Optional[str]
) -> tuple[str, str]:
    """校验 scope 与标识，返回存储用的 (tenant, route)；非法即 422。"""
    tenant = tenant_id or EMPTY
    route = route_id or EMPTY
    if scope == "global":
        return EMPTY, EMPTY
    if scope == "tenant":
        if not tenant:
            raise HTTPException(422, "tenant 作用域必须提供 tenant_id")
        return tenant, EMPTY
    if scope == "route":
        if not tenant or not route:
            raise HTTPException(422, "route 作用域必须同时提供 tenant_id 与 route_id")
        return tenant, route
    raise HTTPException(422, f"非法 scope: {scope!r}")


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
    tenant, route = _normalize_policy_key(scope, tenant_id, route_id)
    if capacity < 0 or refill_rate < 0:
        raise HTTPException(422, "capacity 与 refill_rate 必须非负")
    if low_balance_threshold is not None and not (
        0.0 <= low_balance_threshold <= capacity
    ):
        raise HTTPException(
            422, "low_balance_threshold 必须为 null（关闭）或在 0 到 capacity 之间"
        )

    # 有待生效的定时变更期间，禁止直接 PUT：避免新值被随后的到期变更悄悄覆盖
    pending = conn.execute(
        "SELECT change_id FROM policy_changes "
        "WHERE scope=? AND tenant_id=? AND route_id=? AND status='pending'",
        (scope, tenant, route),
    ).fetchone()
    if pending is not None:
        raise HTTPException(
            409,
            f"该策略存在待生效变更 change_id={pending['change_id']!r}，"
            "请先取消后再直接更新",
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


# ------------------------------------------------------------ 策略定时切换


def _change_out(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "change_id": row["change_id"],
        "scope": row["scope"],
        "tenant_id": row["tenant_id"] or None,
        "route_id": row["route_id"] or None,
        "capacity": row["capacity"],
        "refill_rate": row["refill_rate"],
        "low_balance_threshold": row["low_balance_threshold"],
        "effective_at": row["effective_at"],
        "status": row["status"],
        "created_at": row["created_at"],
        "applied_at": row["applied_at"],
        "cancelled_at": row["cancelled_at"],
    }


def list_policy_changes(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT change_id, scope, tenant_id, route_id, capacity, refill_rate, "
        "low_balance_threshold, effective_at, status, created_at, applied_at, "
        "cancelled_at FROM policy_changes ORDER BY id"
    ).fetchall()
    return [_change_out(r) for r in rows]


def create_policy_change(
    conn: sqlite3.Connection,
    *,
    change_id: str,
    scope: str,
    tenant_id: Optional[str],
    route_id: Optional[str],
    capacity: float,
    refill_rate: float,
    low_balance_threshold: Optional[float],
    effective_at: str,
) -> tuple[dict[str, Any], bool]:
    """登记一条待生效的策略变更，返回 (变更记录, 是否为新建)。

    目标策略必须已存在；effective_at 必须是未来 UTC 时间；同一策略至多一条
    pending 变更（数据库部分唯一索引兜底，违反即 409）。change_id 为幂等键：
    完全相同的变更重复提交返回原记录（created=False，调用方应回 200），
    change_id 已被其他变更占用则 409。
    """
    tenant, route = _normalize_policy_key(scope, tenant_id, route_id)
    if capacity < 0 or refill_rate < 0:
        raise HTTPException(422, "capacity 与 refill_rate 必须非负")
    if low_balance_threshold is not None and not (
        0.0 <= low_balance_threshold <= capacity
    ):
        raise HTTPException(
            422, "low_balance_threshold 必须为 null（关闭）或在 0 到 capacity 之间"
        )
    try:
        eff = parse_iso(effective_at)
    except ValueError:
        raise HTTPException(422, "effective_at 必须是 ISO 8601 时间（UTC）")
    if eff <= now_utc():
        raise HTTPException(422, "effective_at 必须是未来的 UTC 时间")

    eff_text = iso(eff)
    existing = conn.execute(
        "SELECT * FROM policy_changes WHERE change_id=?", (change_id,)
    ).fetchone()
    if existing is not None:
        # 重试时客户端往往重新生成「当前时刻 + 偏移」，允许生效时刻相差 60 秒内
        # 视为同一次调度（真正要改期时可先取消再重提，取消不受此影响）
        eff_match = existing["effective_at"] == eff_text
        if not eff_match:
            try:
                eff_match = (
                    abs(
                        (parse_iso(existing["effective_at"]) - eff).total_seconds()
                    )
                    <= 60
                )
            except ValueError:
                eff_match = False
        threshold = existing["low_balance_threshold"]
        threshold_match = (
            threshold is None
            if low_balance_threshold is None
            else threshold is not None
            and float(threshold) == float(low_balance_threshold)
        )
        same = (
            existing["scope"] == scope
            and existing["tenant_id"] == tenant
            and existing["route_id"] == route
            and float(existing["capacity"]) == float(capacity)
            and float(existing["refill_rate"]) == float(refill_rate)
            and threshold_match
            and eff_match
        )
        if not same:
            raise HTTPException(409, f"change_id={change_id!r} 已被其他变更占用")
        return _change_out(existing), False

    target = conn.execute(
        "SELECT 1 FROM policies WHERE scope=? AND tenant_id=? AND route_id=?",
        (scope, tenant, route),
    ).fetchone()
    if target is None:
        raise HTTPException(404, "目标策略不存在，请先 PUT /policies 创建")

    ts = iso(now_utc())
    try:
        conn.execute(
            """
            INSERT INTO policy_changes
                (change_id, scope, tenant_id, route_id, capacity, refill_rate,
                 low_balance_threshold, effective_at, status, created_at,
                 applied_at, cancelled_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, NULL, NULL)
            """,
            (
                change_id, scope, tenant, route, float(capacity), float(refill_rate),
                None
                if low_balance_threshold is None
                else float(low_balance_threshold),
                eff_text, ts,
            ),
        )
    except sqlite3.IntegrityError:
        # 部分唯一索引：同一策略已有 pending 变更
        raise HTTPException(409, "该策略已存在待生效变更，请先取消或等待生效")
    row = conn.execute(
        "SELECT * FROM policy_changes WHERE change_id=?", (change_id,)
    ).fetchone()
    return _change_out(row), True


def cancel_policy_change(conn: sqlite3.Connection, change_id: str) -> dict[str, Any]:
    """取消一条尚未生效的变更；已生效/已取消/不存在分别 409/幂等/404。"""
    row = conn.execute(
        "SELECT * FROM policy_changes WHERE change_id=?", (change_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(404, f"change_id={change_id!r} 不存在")
    if row["status"] == "applied":
        raise HTTPException(409, f"change_id={change_id!r} 已生效，无法取消")
    if row["status"] == "cancelled":
        return _change_out(row)
    ts = iso(now_utc())
    conn.execute(
        "UPDATE policy_changes SET status='cancelled', cancelled_at=? "
        "WHERE change_id=? AND status='pending'",
        (ts, change_id),
    )
    row = conn.execute(
        "SELECT * FROM policy_changes WHERE change_id=?", (change_id,)
    ).fetchone()
    return _change_out(row)


def apply_due_policy_changes(conn: sqlite3.Connection, now: datetime) -> None:
    """套用所有 effective_at <= now 的待生效变更。

    必须在裁决/撤销的写事务开头、读取桶之前调用，使「变更落库 + 本次裁决」
    在同一 BEGIN IMMEDIATE 事务内原子完成，并发请求只能看到完整的旧或新状态。

    每个到期变更对（策略 + 桶）的处理：
      1. 桶先按【旧策略】补充到 effective_at；
      2. 余额按容量等比缩放 旧余额 / 旧容量 × 新容量
         （旧容量为 0 时余额直接为 0；新容量 0 自然也得 0）；
      3. 桶的 updated_at 锚定到 effective_at，随后调用方再按【新速率】
         从 effective_at 补充到裁决时刻。
    """
    now_text = iso(now)
    due = conn.execute(
        """
        SELECT change_id, scope, tenant_id, route_id,
               capacity, refill_rate, low_balance_threshold, effective_at
        FROM policy_changes
        WHERE status='pending' AND effective_at <= ?
        ORDER BY effective_at, id
        """,
        (now_text,),
    ).fetchall()

    for ch in due:
        scope, tenant, route = ch["scope"], ch["tenant_id"], ch["route_id"]
        eff = parse_iso(ch["effective_at"])
        eff_text = ch["effective_at"]
        new_cap, new_rate = float(ch["capacity"]), float(ch["refill_rate"])
        new_threshold = ch["low_balance_threshold"]

        # 同一策略正常只会有一条 pending；但同一事务里若有多条到期，
        # 旧值必须在循环内实时读取（上一条变更已经更新过 policies/buckets）
        pol = conn.execute(
            "SELECT capacity, refill_rate FROM policies "
            "WHERE scope=? AND tenant_id=? AND route_id=?",
            (scope, tenant, route),
        ).fetchone()
        if pol is None:
            # 目标策略在登记后被删除（当前版本不支持删除，仅作防御）：
            # 标记变更但无桶可迁移
            conn.execute(
                "UPDATE policy_changes SET status='applied', applied_at=? "
                "WHERE change_id=?",
                (eff_text, ch["change_id"]),
            )
            continue
        old_cap, old_rate = float(pol["capacity"]), float(pol["refill_rate"])

        bucket = conn.execute(
            "SELECT balance, updated_at FROM buckets "
            "WHERE scope=? AND tenant_id=? AND route_id=?",
            (scope, tenant, route),
        ).fetchone()
        if bucket is None:
            # 桶尚未惰性创建：生效后首次命中按新容量满额建桶，无需迁移余额
            balance_at_eff = new_cap
            conn.execute(
                "INSERT INTO buckets (scope, tenant_id, route_id, balance, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (scope, tenant, route, balance_at_eff, eff_text),
            )
        else:
            # 1) 旧策略补充到 effective_at
            balance = _refill(
                bucket["balance"], bucket["updated_at"],
                old_cap, old_rate, eff,
            )
            # 2) 按容量等比缩放剩余令牌（旧容量为 0 -> 余额 0）
            if old_cap > 0:
                balance = balance / old_cap * new_cap
            else:
                balance = 0.0
            # 3) 锚定到生效时刻；新速率的补充留给调用方从 eff 算到 now
            conn.execute(
                "UPDATE buckets SET balance=?, updated_at=? "
                "WHERE scope=? AND tenant_id=? AND route_id=?",
                (balance, eff_text, scope, tenant, route),
            )

        conn.execute(
            """
            UPDATE policies SET capacity=?, refill_rate=?,
                low_balance_threshold=?, updated_at=?
            WHERE scope=? AND tenant_id=? AND route_id=?
            """,
            (
                new_cap, new_rate, new_threshold, eff_text,
                scope, tenant, route,
            ),
        )
        conn.execute(
            "UPDATE policy_changes SET status='applied', applied_at=? "
            "WHERE change_id=?",
            (eff_text, ch["change_id"]),
        )


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
    """列出所有已创建的桶；live=True 时展示补充到当前时刻的余额（不落库）。

    只读投影：若某桶存在已到期但尚未被裁决/撤销套用的变更，展示值同样按
    「旧策略补充到 effective_at -> 等比缩放 -> 新速率补充到现在」计算，
    但不写库（真正落库仍发生在下一次写事务内）。
    """
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
        scope, tenant, route = r["scope"], r["tenant_id"], r["route_id"]
        capacity, refill_rate = float(r["capacity"]), float(r["refill_rate"])
        balance = r["balance"]
        updated_at = r["updated_at"]
        if live:
            due = conn.execute(
                "SELECT capacity, refill_rate, effective_at FROM policy_changes "
                "WHERE scope=? AND tenant_id=? AND route_id=? "
                "AND status='pending' AND effective_at <= ?",
                (scope, tenant, route, iso(now)),
            ).fetchone()
            if due is not None:
                eff = parse_iso(due["effective_at"])
                new_cap = float(due["capacity"])
                # 旧策略补到生效时刻 -> 等比缩放 -> 新速率补到现在
                balance = _refill(
                    balance, updated_at, capacity, refill_rate, eff
                )
                balance = (
                    balance / capacity * new_cap if capacity > 0 else 0.0
                )
                balance = _refill(
                    balance, due["effective_at"],
                    new_cap, float(due["refill_rate"]), now,
                )
                capacity, refill_rate = new_cap, float(due["refill_rate"])
            else:
                balance = _refill(
                    balance, updated_at, capacity, refill_rate, now
                )
        out.append(
            _bucket_out(
                scope, tenant, route,
                capacity, refill_rate, balance,
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

    事务开头先套用所有到期的策略定时变更（与本次裁决原子提交），随后命中的
    每个桶在同一时刻 now 下：
      1. 按距上次更新经过的时间补充令牌（不超过容量）；
      2. 全部桶余额都足够才统一扣减；任一不足则一个都不扣。
    即使本次是 request_id 重放，到期变更也照常套用，保证任何裁决调用都会
    推动状态前进，且并发请求只能观察到完整的旧或新状态。
    """
    if cost <= 0:
        raise HTTPException(422, "cost 必须为正数")
    if route_id and not tenant_id:
        raise HTTPException(422, "提供 route_id 时必须同时提供 tenant_id")

    now = now_utc()
    # 0) 先在本事务内套用所有到期的策略变更；即使随后走幂等重放也已完成切换
    apply_due_policy_changes(conn, now)

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

    # 3) 取桶（惰性创建，新桶满额），并在同一 now 下补充。
    #    到期变更已把桶 updated_at 锚定到 effective_at，此处自然按新速率
    #    从生效时刻补充到 now。
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

    now = now_utc()
    # 与裁决相同：本事务内先套用所有到期的策略变更（重复撤销也不例外），
    # 再按（可能已更新的）策略补充/退还
    apply_due_policy_changes(conn, now)

    if row["revoked"]:
        result = json.loads(row["result_json"])
        result["replayed"] = True
        return result, True

    payload = json.loads(row["payload_json"])
    cost = float(row["cost"])

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
