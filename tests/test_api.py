"""核心行为测试。用 QUOTA_DB_PATH 指向每个测试独立的临时 SQLite 文件。"""

from __future__ import annotations

import os
import threading

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch) -> TestClient:
    db_file = tmp_path / "test.db"
    monkeypatch.setenv("QUOTA_DB_PATH", str(db_file))
    # 延迟导入 app，确保它读到环境变量
    import importlib

    from app import database, main

    importlib.reload(database)
    importlib.reload(main)
    database.init_db(str(db_file))
    with TestClient(main.app) as c:
        yield c


_UNSET = object()


def put_policy(client, scope, capacity, refill_rate, tenant=None, route=None,
               threshold=_UNSET):
    body = {
        "scope": scope,
        "tenant_id": tenant,
        "route_id": route,
        "capacity": capacity,
        "refill_rate": refill_rate,
    }
    if threshold is not _UNSET:
        body["low_balance_threshold"] = threshold
    return client.put("/policies", json=body)


# ------------------------------------------------------------ 基本扣减与拒绝


def test_allow_and_atomic_deny_multilevel(client):
    assert put_policy(client, "global", 5, 0).status_code == 200
    assert put_policy(client, "tenant", 3, 0, tenant="t1").status_code == 200
    assert (
        put_policy(client, "route", 4, 0, tenant="t1", route="rA").status_code == 200
    )

    r = client.post(
        "/decide",
        json={"request_id": "d1", "tenant_id": "t1", "route_id": "rA", "cost": 3},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["allowed"] is True
    assert body["replayed"] is False
    by_scope = {b["scope"]: b["balance"] for b in body["buckets"]}
    assert by_scope == {"global": 2.0, "tenant": 0.0, "route": 1.0}

    # 租户桶已空 -> 拒绝；全局/路由桶一个都不应被扣
    r = client.post(
        "/decide",
        json={"request_id": "d2", "tenant_id": "t1", "route_id": "rA", "cost": 1},
    )
    body = r.json()
    assert body["allowed"] is False
    assert body["retry_possible"] is False  # refill_rate=0 永不可能
    assert body["retry_at"] is None
    by_scope = {b["scope"]: b["balance"] for b in body["buckets"]}
    assert by_scope == {"global": 2.0, "tenant": 0.0, "route": 1.0}


def test_deny_does_not_deduct_any_bucket(client):
    put_policy(client, "global", 2, 0)
    put_policy(client, "tenant", 10, 0, tenant="t")
    r = client.post(
        "/decide", json={"request_id": "x", "tenant_id": "t", "cost": 3}
    ).json()
    assert r["allowed"] is False
    # 全局桶容量只有 2，但绝不能被扣成负数
    global_b = [b for b in r["buckets"] if b["scope"] == "global"][0]
    tenant_b = [b for b in r["buckets"] if b["scope"] == "tenant"][0]
    assert global_b["balance"] == 2.0
    assert tenant_b["balance"] == 10.0


# ------------------------------------------------------------ 时间补充与重试时间


def test_refill_over_time(client):
    put_policy(client, "global", 10, 10)  # 10 令牌/秒
    # 连续打空
    for i in range(10):
        r = client.post("/decide", json={"request_id": f"e{i}", "cost": 1}).json()
        assert r["allowed"] is True
    r = client.post("/decide", json={"request_id": "e-empty", "cost": 1}).json()
    assert r["allowed"] is False
    assert r["retry_possible"] is True
    assert r["retry_after_seconds"] == pytest.approx(0.1, abs=0.25)
    import time

    time.sleep(0.3)
    r = client.post("/decide", json={"request_id": "e-refill", "cost": 1}).json()
    assert r["allowed"] is True


def test_retry_time_is_max_across_buckets(client):
    # 先打一笔 cost=6：全局 6->0（缺 4 @4/s=1s），租户 8->2（缺 2 @2/s=1s）
    # 再请求 cost=4 被拒，整组最早可重试时间取最大值 1s
    put_policy(client, "global", 6, 4)
    put_policy(client, "tenant", 8, 2, tenant="t")
    client.post("/decide", json={"request_id": "a", "tenant_id": "t", "cost": 6})
    r = client.post(
        "/decide", json={"request_id": "b", "tenant_id": "t", "cost": 4}
    ).json()
    assert r["allowed"] is False
    assert r["retry_after_seconds"] == pytest.approx(1.0, abs=0.5)
    assert r["retry_at"] is not None


def test_cost_above_capacity_means_impossible(client):
    put_policy(client, "global", 3, 10)
    r = client.post("/decide", json={"request_id": "big", "cost": 5}).json()
    assert r["allowed"] is False
    assert r["retry_possible"] is False
    assert r["retry_at"] is None


# ------------------------------------------------------------ 幂等


def test_idempotent_replay_same_payload(client):
    put_policy(client, "global", 1, 0)
    payload = {"request_id": "id1", "cost": 1}
    first = client.post("/decide", json=payload).json()
    assert first["allowed"] is True
    second = client.post("/decide", json=payload).json()
    assert second["allowed"] is True
    assert second["replayed"] is True
    # 余额没有因为“再来一次”被扣两次
    assert second["buckets"][0]["balance"] == 0.0
    assert second["decided_at"] == first["decided_at"]


def test_idempotent_replay_denied(client):
    put_policy(client, "global", 1, 0)
    client.post("/decide", json={"request_id": "n1", "cost": 1})
    denied = client.post(
        "/decide", json={"request_id": "n2", "cost": 1}
    ).json()
    assert denied["allowed"] is False
    replay = client.post(
        "/decide", json={"request_id": "n2", "cost": 1}
    ).json()
    assert replay["allowed"] is False
    assert replay["replayed"] is True


def test_payload_conflict_returns_409(client):
    put_policy(client, "global", 100, 0)
    client.post("/decide", json={"request_id": "k1", "cost": 1})
    r = client.post("/decide", json={"request_id": "k1", "cost": 2})
    assert r.status_code == 409


# ------------------------------------------------------------ 撤销


def test_revoke_refunds_and_is_idempotent(client):
    put_policy(client, "global", 10, 0)
    put_policy(client, "tenant", 10, 0, tenant="t")
    client.post(
        "/decide", json={"request_id": "rv", "tenant_id": "t", "cost": 4}
    )
    r = client.post("/decide/rv/revoke")
    assert r.status_code == 200
    body = r.json()
    assert body["revoked"] is True
    assert all(b["balance"] == 10.0 for b in body["buckets"])

    # 重复撤销幂等
    r2 = client.post("/decide/rv/revoke")
    assert r2.status_code == 200
    assert r2.json()["revoked"] is True

    # 令牌已补回，新的裁决可以通过；不会因为撤销两次而多出令牌
    ok = client.post(
        "/decide", json={"request_id": "rv-next", "tenant_id": "t", "cost": 10}
    ).json()
    assert ok["allowed"] is True


def test_revoke_refund_capped_at_capacity(client):
    put_policy(client, "global", 10, 100)  # 高补充率：撤销前桶可能早已回满
    client.post("/decide", json={"request_id": "c1", "cost": 3})
    body = client.post("/decide/c1/revoke").json()
    assert all(b["balance"] <= b["capacity"] for b in body["buckets"])
    assert body["buckets"][0]["balance"] == 10.0


def test_revoke_denied_decision_conflicts(client):
    put_policy(client, "global", 1, 0)
    client.post("/decide", json={"request_id": "ok", "cost": 1})
    client.post("/decide", json={"request_id": "no", "cost": 1})
    r = client.post("/decide/no/revoke")
    assert r.status_code == 409


def test_revoke_unknown_404(client):
    assert client.post("/decide/nope/revoke").status_code == 404


# ------------------------------------------------------------ 策略更新


def test_policy_update_clamps_balance(client):
    put_policy(client, "global", 100, 0)
    client.post("/decide", json={"request_id": "p1", "cost": 10})
    # 容量缩小到 5：余额 90 被截断到 5
    put_policy(client, "global", 5, 0)
    r = client.post("/decide", json={"request_id": "p2", "cost": 6}).json()
    assert r["allowed"] is False  # 5 < 6，且 rate=0
    assert r["buckets"][0]["balance"] == 5.0


# ------------------------------------------------------------ 并发不超发


def test_concurrent_decisions_never_overspend(client):
    capacity = 50
    put_policy(client, "global", capacity, 0)

    n_threads, per_thread = 20, 5
    results: list[bool] = []
    lock = threading.Lock()

    def worker(idx):
        for i in range(per_thread):
            body = client.post(
                "/decide",
                json={"request_id": f"c-{idx}-{i}", "cost": 1},
            ).json()
            with lock:
                results.append(body["allowed"])

    threads = [threading.Thread(target=worker, args=(j,)) for j in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 恰好 capacity 个通过，其余拒绝；余额精确为 0，即无超发也无少发
    assert sum(results) == capacity
    buckets = client.get("/buckets").json()
    assert buckets[0]["balance"] == 0


def test_concurrent_multilevel_never_overspend(client):
    put_policy(client, "global", 30, 0)
    put_policy(client, "tenant", 12, 0, tenant="t")
    results: list[bool] = []
    lock = threading.Lock()

    def worker(idx):
        for i in range(5):
            body = client.post(
                "/decide",
                json={"request_id": f"m-{idx}-{i}", "tenant_id": "t", "cost": 1},
            ).json()
            with lock:
                results.append(body["allowed"])

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sum(results) == 12  # 租户桶上限 12，不可能更多
    buckets = {b["scope"]: b["balance"] for b in client.get("/buckets").json()}
    assert buckets["tenant"] == 0
    assert buckets["global"] == 30 - 12


def test_validation_errors(client):
    # route 必须带 tenant
    r = client.post("/decide", json={"request_id": "v", "route_id": "r", "cost": 1})
    assert r.status_code == 422
    # 无策略可命中
    r = client.post("/decide", json={"request_id": "v2", "cost": 1})
    assert r.status_code == 404
    # cost 必须为正
    r = client.post("/decide", json={"request_id": "v3", "cost": 0})
    assert r.status_code == 422


# ------------------------------------------------------------ 低余额事件


def test_threshold_validation(client):
    # 负数、超过 capacity 都非法
    assert put_policy(client, "global", 10, 0, threshold=-0.1).status_code == 422
    assert put_policy(client, "global", 10, 0, threshold=10.5).status_code == 422
    # null（关闭）、0、等于 capacity 都合法
    assert put_policy(client, "global", 10, 0, threshold=None).status_code == 200
    assert put_policy(client, "global", 10, 0, threshold=0).status_code == 200
    r = put_policy(client, "global", 10, 0, threshold=10)
    assert r.status_code == 200
    assert r.json()["low_balance_threshold"] == 10.0
    # PUT 全量替换：不传字段即关闭
    r = put_policy(client, "global", 10, 0)
    assert r.json()["low_balance_threshold"] is None
    listed = client.get("/policies").json()
    assert listed[0]["low_balance_threshold"] is None


def test_low_balance_event_on_crossing(client):
    put_policy(client, "global", 10, 0, threshold=4)
    # 10 -> 7：仍在阈值之上，无事件
    client.post("/decide", json={"request_id": "lb1", "cost": 3})
    assert client.get("/events").json() == []
    # 7 -> 4：恰好降到阈值（不高于阈值），产生唯一事件
    client.post("/decide", json={"request_id": "lb2", "cost": 3})
    events = client.get("/events").json()
    assert len(events) == 1
    ev = events[0]
    assert ev["id"] == 1
    assert ev["scope"] == "global"
    assert ev["tenant_id"] is None
    assert ev["route_id"] is None
    assert ev["balance_before"] == 7.0
    assert ev["balance_after"] == 4.0
    assert ev["threshold"] == 4.0
    assert ev["request_id"] == "lb2"
    assert ev["created_at"]


def test_no_event_when_already_below_threshold(client):
    put_policy(client, "global", 10, 0, threshold=4)
    client.post("/decide", json={"request_id": "a", "cost": 6})  # 10 -> 4，越线
    client.post("/decide", json={"request_id": "b", "cost": 2})  # 4 -> 2，不越线
    client.post("/decide", json={"request_id": "c", "cost": 2})  # 2 -> 0，不越线
    events = client.get("/events").json()
    assert [e["request_id"] for e in events] == ["a"]


def test_no_event_on_deny_or_replay(client):
    put_policy(client, "global", 5, 0, threshold=2)
    client.post("/decide", json={"request_id": "ok", "cost": 4})  # 5 -> 1，事件
    denied = client.post("/decide", json={"request_id": "no", "cost": 4}).json()
    assert denied["allowed"] is False
    replay = client.post("/decide", json={"request_id": "ok", "cost": 4}).json()
    assert replay["replayed"] is True
    events = client.get("/events").json()
    assert [e["request_id"] for e in events] == ["ok"]


def test_event_rearms_after_refill(client):
    put_policy(client, "global", 10, 100, threshold=5)  # 100 令牌/秒，迅速回满
    client.post("/decide", json={"request_id": "r1", "cost": 8})  # 10 -> 2，事件
    import time

    time.sleep(0.2)  # 时间补充把余额重新抬到阈值之上
    client.post("/decide", json={"request_id": "r2", "cost": 8})  # 再次越线
    events = client.get("/events").json()
    assert [e["request_id"] for e in events] == ["r1", "r2"]
    assert events[0]["id"] < events[1]["id"]


def test_event_rearms_after_revoke(client):
    put_policy(client, "global", 10, 0, threshold=5)
    client.post("/decide", json={"request_id": "v1", "cost": 8})  # 10 -> 2，事件
    client.post("/decide/v1/revoke")  # 撤销补回 -> 10
    client.post("/decide", json={"request_id": "v2", "cost": 8})  # 再次越线
    events = client.get("/events").json()
    assert [e["request_id"] for e in events] == ["v1", "v2"]


def test_event_rearms_after_threshold_lowered(client):
    put_policy(client, "global", 10, 0, threshold=5)
    client.post("/decide", json={"request_id": "t1", "cost": 8})  # 10 -> 2，事件
    # 策略更新把阈值调到 1：余额 2 重新高于阈值
    put_policy(client, "global", 10, 0, threshold=1)
    client.post("/decide", json={"request_id": "t2", "cost": 2})  # 2 -> 0，再越线
    events = client.get("/events").json()
    assert [e["request_id"] for e in events] == ["t1", "t2"]
    assert events[1]["threshold"] == 1.0


def test_multilevel_crossing_emits_one_event_per_bucket(client):
    put_policy(client, "global", 10, 0, threshold=5)
    put_policy(client, "tenant", 8, 0, tenant="acme", threshold=3)
    put_policy(client, "route", 6, 0, tenant="acme", route="/r")  # 阈值关闭
    client.post(
        "/decide",
        json={"request_id": "m1", "tenant_id": "acme", "route_id": "/r", "cost": 6},
    )
    # global 10->4 越线、tenant 8->2 越线、route 未配置阈值 -> 恰好 2 个事件
    events = client.get("/events").json()
    assert [(e["scope"], e["request_id"]) for e in events] == [
        ("global", "m1"),
        ("tenant", "m1"),
    ]
    by_scope = {e["scope"]: e for e in events}
    assert by_scope["global"]["balance_after"] == 4.0
    assert by_scope["tenant"]["balance_after"] == 2.0
    assert by_scope["tenant"]["tenant_id"] == "acme"


def test_events_pagination_stable(client):
    put_policy(client, "global", 3, 0, threshold=0)
    # 打空 -> 撤销补回 -> 再打空，循环 3 次，产生 3 个事件
    for i in range(3):
        client.post("/decide", json={"request_id": f"p{i}", "cost": 3})
        client.post(f"/decide/p{i}/revoke")

    page1 = client.get("/events", params={"limit": 2}).json()
    assert [e["id"] for e in page1] == [1, 2]
    page2 = client.get(
        "/events", params={"after_id": page1[-1]["id"], "limit": 2}
    ).json()
    assert [e["id"] for e in page2] == [3]
    page3 = client.get(
        "/events", params={"after_id": page2[-1]["id"], "limit": 2}
    ).json()
    assert page3 == []
    # 参数校验
    assert client.get("/events", params={"after_id": -1}).status_code == 422
    assert client.get("/events", params={"limit": 0}).status_code == 422


def test_old_database_is_migrated(tmp_path):
    """老库（policies 无 low_balance_threshold 列）init_db 后读写正常。"""
    import sqlite3

    from app import database, quota

    db_file = tmp_path / "old.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute(
        "CREATE TABLE policies ("
        "  scope TEXT NOT NULL, tenant_id TEXT NOT NULL DEFAULT '',"
        "  route_id TEXT NOT NULL DEFAULT '', capacity REAL NOT NULL,"
        "  refill_rate REAL NOT NULL, updated_at TEXT NOT NULL,"
        "  PRIMARY KEY (scope, tenant_id, route_id)) STRICT"
    )
    conn.execute(
        "INSERT INTO policies VALUES ('global', '', '', 10, 1, '2026-01-01T00:00:00Z')"
    )
    conn.commit()
    conn.close()

    database.init_db(str(db_file))
    conn = database.connect(str(db_file))
    try:
        policies = quota.list_policies(conn)
        # 老数据阈值视为关闭；新列可正常 upsert
        quota.upsert_policy(
            conn, scope="global", tenant_id=None, route_id=None,
            capacity=10, refill_rate=1, low_balance_threshold=5,
        )
        updated = quota.list_policies(conn)
    finally:
        conn.close()
    assert policies[0]["low_balance_threshold"] is None
    assert updated[0]["low_balance_threshold"] == 5.0
