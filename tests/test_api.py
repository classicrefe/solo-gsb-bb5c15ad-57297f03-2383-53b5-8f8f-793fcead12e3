"""核心行为测试。用 QUOTA_DB_PATH 指向每个测试独立的临时 SQLite 文件。"""

from __future__ import annotations

import os
import threading
from datetime import datetime, timedelta, timezone

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


def iso_in(seconds: float) -> str:
    """未来（或负秒数的过去）UTC ISO 文本。"""
    return (
        (datetime.now(timezone.utc) + timedelta(seconds=seconds))
        .isoformat()
        .replace("+00:00", "Z")
    )


def post_change(client, change_id, scope, capacity, refill_rate, effective_in,
                tenant=None, route=None, threshold=_UNSET):
    body = {
        "change_id": change_id,
        "scope": scope,
        "tenant_id": tenant,
        "route_id": route,
        "capacity": capacity,
        "refill_rate": refill_rate,
        "effective_at": iso_in(effective_in),
    }
    if threshold is not _UNSET:
        body["low_balance_threshold"] = threshold
    return client.post("/policy-changes", json=body)


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


# ------------------------------------------------------------ 策略定时切换


def test_create_change_requires_existing_policy_and_future_time(client):
    # 目标策略不存在 -> 404
    r = post_change(client, "c1", "global", 20, 1, effective_in=10)
    assert r.status_code == 404

    put_policy(client, "global", 10, 0)
    # 过去时间 -> 422
    r = post_change(client, "c2", "global", 20, 1, effective_in=-10)
    assert r.status_code == 422
    # 非法时间文本 -> 422
    r = client.post(
        "/policy-changes",
        json={
            "change_id": "c3", "scope": "global", "capacity": 20,
            "refill_rate": 1, "effective_at": "not-a-time",
        },
    )
    assert r.status_code == 422
    # 阈值超范围 -> 422
    r = post_change(client, "c4", "global", 20, 1, effective_in=10, threshold=21)
    assert r.status_code == 422


def test_create_change_success_and_idempotent(client):
    put_policy(client, "global", 10, 0)
    payload = {
        "change_id": "ch-1", "scope": "global", "capacity": 20,
        "refill_rate": 2, "low_balance_threshold": 5,
        "effective_at": iso_in(60),
    }
    r = client.post("/policy-changes", json=payload)
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "pending"
    assert body["capacity"] == 20.0
    assert body["refill_rate"] == 2.0
    assert body["low_balance_threshold"] == 5.0
    assert body["effective_at"].endswith("Z") or "+" in body["effective_at"]
    assert body["applied_at"] is None
    assert body["cancelled_at"] is None

    # 同载荷重复提交：幂等返回同一条
    again = client.post("/policy-changes", json=payload)
    assert again.status_code == 200
    assert again.json()["change_id"] == "ch-1"
    assert again.json()["status"] == "pending"

    # 相同 change_id 但参数不同 -> 409
    other = dict(payload, capacity=30)
    r = client.post("/policy-changes", json=other)
    assert r.status_code == 409


def test_one_pending_change_per_policy_and_put_conflict(client):
    put_policy(client, "global", 10, 0)
    assert post_change(client, "a", "global", 20, 1, 10).status_code == 201
    # 第二条待生效变更 -> 409
    assert post_change(client, "b", "global", 30, 1, 10).status_code == 409
    # 待生效期间 PUT 直接更新 -> 409
    r = put_policy(client, "global", 99, 0)
    assert r.status_code == 409
    # 其他不受影响的策略仍可 PUT
    assert put_policy(client, "tenant", 5, 0, tenant="t").status_code == 200

    # 取消后：PUT 恢复可用，也能登记新的变更
    assert client.delete("/policy-changes/a").status_code == 200
    assert put_policy(client, "global", 99, 0).status_code == 200
    r = post_change(client, "c", "global", 8, 0, effective_in=10)
    assert r.status_code == 201


def test_cancel_change_lifecycle(client):
    put_policy(client, "global", 10, 0)
    post_change(client, "x", "global", 20, 1, effective_in=10)
    # 不存在 -> 404
    assert client.delete("/policy-changes/nope").status_code == 404
    # 取消成功
    r = client.delete("/policy-changes/x")
    assert r.status_code == 200
    assert r.json()["status"] == "cancelled"
    assert r.json()["cancelled_at"]
    # 重复取消幂等
    r2 = client.delete("/policy-changes/x")
    assert r2.status_code == 200
    assert r2.json()["status"] == "cancelled"
    # 取消后该变更永不到期生效
    import time

    time.sleep(0.2)
    client.post("/decide", json={"request_id": "d", "cost": 1})
    pol = client.get("/policies").json()[0]
    assert pol["capacity"] == 10.0
    listed = client.get("/policy-changes").json()
    assert [c["status"] for c in listed] == ["cancelled"]


def test_change_not_applied_before_effective_at(client):
    put_policy(client, "global", 10, 0)
    client.post("/decide", json={"request_id": "d0", "cost": 6})  # 余额 4
    post_change(client, "future", "global", 20, 100, effective_in=30)
    # 立即裁决：变更未到期，仍按旧策略（容量 10、速率 0）
    r = client.post("/decide", json={"request_id": "d1", "cost": 5}).json()
    assert r["allowed"] is False
    assert r["buckets"][0]["capacity"] == 10.0
    assert r["buckets"][0]["balance"] == 4.0
    ch = client.get("/policy-changes").json()[0]
    assert ch["status"] == "pending"


def test_change_applied_on_decide_with_ratio_and_new_rate(client):
    # 旧容量 10、速率 0；打一笔后余额 4
    put_policy(client, "global", 10, 0)
    client.post("/decide", json={"request_id": "d0", "cost": 6})
    # 0.2s 后生效：新容量 20、新速率 100/s
    post_change(client, "grow", "global", 20, 100, effective_in=0.2)
    import time

    time.sleep(0.35)
    # 生效时刻余额 = 4/10*20 = 8；之后按 100/s 补充并截断到新容量 20
    r = client.post("/decide", json={"request_id": "d1", "cost": 1}).json()
    assert r["allowed"] is True
    bucket = r["buckets"][0]
    assert bucket["capacity"] == 20.0
    assert bucket["refill_rate"] == 100.0
    assert bucket["balance"] == pytest.approx(19.0, abs=0.01)
    ch = client.get("/policy-changes").json()[0]
    assert ch["status"] == "applied"
    assert ch["applied_at"] is not None
    pol = client.get("/policies").json()[0]
    assert pol["capacity"] == 20.0
    assert pol["refill_rate"] == 100.0


def test_change_shrink_ratio_with_zero_rate(client):
    # 旧容量 10、速率 0，余额 6；缩容到 5：生效时余额 = 6/10*5 = 3
    put_policy(client, "global", 10, 0)
    client.post("/decide", json={"request_id": "d0", "cost": 4})
    post_change(client, "shrink", "global", 5, 0, effective_in=0.2)
    import time

    time.sleep(0.35)
    r = client.post("/decide", json={"request_id": "d1", "cost": 4}).json()
    assert r["allowed"] is False
    assert r["buckets"][0]["capacity"] == 5.0
    assert r["buckets"][0]["balance"] == pytest.approx(3.0, abs=1e-9)
    assert client.get("/policy-changes").json()[0]["status"] == "applied"


def test_change_old_capacity_zero_sets_balance_zero(client):
    # 旧容量 0：任何请求都不可能通过；裁决调用会惰性创建一个余额 0 的空桶
    put_policy(client, "global", 0, 0)
    denied = client.post("/decide", json={"request_id": "d0", "cost": 1}).json()
    assert denied["allowed"] is False
    post_change(client, "open", "global", 10, 0, effective_in=0.2)
    import time

    time.sleep(0.35)
    # 旧容量为 0 -> 生效时余额为 0（不是满额），cost=1 仍被拒
    r = client.post("/decide", json={"request_id": "d1", "cost": 1}).json()
    assert r["allowed"] is False
    assert r["buckets"][0]["capacity"] == 10.0
    assert r["buckets"][0]["balance"] == 0.0


def test_change_applied_on_replay_and_revoke(client):
    put_policy(client, "global", 10, 10)
    client.post("/decide", json={"request_id": "d0", "cost": 8})  # 余 2
    post_change(client, "g", "global", 20, 10, effective_in=0.2)
    import time

    time.sleep(0.35)
    # 重放旧裁决也必须推动到期变更落库
    replay = client.post(
        "/decide", json={"request_id": "d0", "cost": 8}
    ).json()
    assert replay["replayed"] is True
    assert client.get("/policy-changes").json()[0]["status"] == "applied"
    # 撤销在新策略下补充并退还（结果不超过新容量 20）
    rv = client.post("/decide/d0/revoke").json()
    assert rv["revoked"] is True
    assert rv["buckets"][0]["capacity"] == 20.0
    assert rv["buckets"][0]["balance"] <= 20.0


def test_change_low_balance_threshold_switches(client):
    # 旧阈值 5：10 -> 6 不越线；变更把阈值降到 3 且扩容到 20
    put_policy(client, "global", 10, 0, threshold=5)
    client.post("/decide", json={"request_id": "d0", "cost": 4})  # 余 6
    post_change(
        client, "t", "global", 20, 0, effective_in=0.2, threshold=3
    )
    import time

    time.sleep(0.35)
    # 生效时余额 6/10*20 = 12；扣 10 后余 2，按【新阈值 3】越线 -> 一条事件
    client.post("/decide", json={"request_id": "d1", "cost": 10})
    events = client.get("/events").json()
    assert len(events) == 1
    assert events[0]["request_id"] == "d1"
    assert events[0]["threshold"] == 3.0
    assert events[0]["balance_after"] == pytest.approx(2.0, abs=1e-9)


def test_buckets_endpoint_projects_due_change_without_writes(client):
    put_policy(client, "global", 10, 0)
    client.post("/decide", json={"request_id": "d0", "cost": 5})  # 余 5
    post_change(client, "g", "global", 20, 0, effective_in=0.2)
    import time

    time.sleep(0.35)
    # /buckets 是只读投影：展示等比缩放后的新状态，但不真正套用
    buckets = client.get("/buckets").json()
    assert buckets[0]["capacity"] == 20.0
    assert buckets[0]["balance"] == pytest.approx(10.0, abs=1e-9)
    assert client.get("/policy-changes").json()[0]["status"] == "pending"
    # 下一次裁决在事务内真正落库
    client.post("/decide", json={"request_id": "d1", "cost": 1})
    assert client.get("/policy-changes").json()[0]["status"] == "applied"


def test_concurrent_change_submission_only_one_wins(client):
    put_policy(client, "global", 10, 0)
    statuses: list[int] = []
    lock = threading.Lock()

    def submit(idx):
        r = post_change(
            client, f"cc-{idx}", "global", 20, 1, effective_in=600
        )
        with lock:
            statuses.append(r.status_code)

    threads = [threading.Thread(target=submit, args=(i,)) for i in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 恰好一条 201，其余全部 409（部分唯一索引串行化兜底）
    assert sorted(statuses).count(201) == 1
    assert sorted(statuses).count(409) == 15
    pending = [
        c for c in client.get("/policy-changes").json() if c["status"] == "pending"
    ]
    assert len(pending) == 1


def test_concurrent_decisions_apply_change_atomically(client):
    # 旧容量 20、速率 0；所有线程在生效点附近打余额。切换到容量 10 时
    # 余额按比例缩放；任何时刻并发裁决只能看到完整的旧状态或完整新状态。
    put_policy(client, "global", 20, 0)
    post_change(client, "switch", "global", 10, 0, effective_in=0.5)

    n_threads, per_thread = 16, 2
    allowed: list[bool] = []
    lock = threading.Lock()

    def worker(idx):
        import time

        time.sleep(0.5)  # 与生效时刻并发涌入
        for i in range(per_thread):
            body = client.post(
                "/decide",
                json={"request_id": f"pc-{idx}-{i}", "cost": 1},
            ).json()
            with lock:
                allowed.append(body["allowed"])

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 每个返回桶的容量要么全是旧值 20，要么全是新值 10（此处只有一级），
    # 且总通过数不可能超过旧容量 20
    assert sum(allowed) <= 20
    buckets = client.get("/buckets").json()
    assert buckets[0]["capacity"] == 10.0
    assert 0.0 <= buckets[0]["balance"] <= 10.0
    assert client.get("/policy-changes").json()[0]["status"] == "applied"


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
