#!/usr/bin/env python3
"""端到端演示脚本（仅用标准库，容器/本地均可运行）。

流程：
1. 重置模拟站，设置详情延迟与“首次失败”
2. 启动采集任务，详情开始入库后，在慢响应下连续多次 暂停 -> 恢复
3. 验证：
   - 暂停不等慢请求结束，暂停瞬间进度仍计入旧轮次在途请求
   - 暂停响应返回后旧轮次不再写入（等旧请求落地后记录数不变）
   - 立即恢复被受理，新旧轮次合计在途任何时刻 <= 2（公示站侧佐证）
   - 并发恢复返回 409，同任务只启动一轮
   - 首次失败项经恢复后补采成功，已成功记录不重采
4. 全部入库后校验记录数（去重后 35 条）与字段完整性

环境变量：
  COLLECTOR_URL  采集 API 地址，默认 http://localhost:8000
  MOCK_SITE_URL  模拟站地址，默认 http://localhost:8001
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

COLLECTOR = os.environ.get("COLLECTOR_URL", "http://localhost:8000").rstrip("/")
MOCK = os.environ.get("MOCK_SITE_URL", "http://localhost:8001").rstrip("/")

DETAIL_DELAY = 0.6  # 公示站慢响应：每个详情请求耗时 0.6s


def call(method: str, url: str, expect=(200, 201)) -> dict:
    req = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            body = resp.read().decode()
            status = resp.status
    except urllib.error.HTTPError as exc:
        body = exc.read().decode()
        status = exc.code
    data = json.loads(body) if body else {}
    if status not in expect:
        raise AssertionError(f"{method} {url} -> {status}: {body}")
    return data


def progress(task_id: str) -> dict:
    return call("GET", f"{COLLECTOR}/api/tasks/{task_id}/progress")


def wait_status(task_id: str, acceptable: set[str], timeout: float = 30) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        prog = progress(task_id)
        if prog["status"] in acceptable:
            return prog
        time.sleep(0.2)
    raise TimeoutError(f"task {task_id} never reached {acceptable}")


def wait_success_at_least(task_id: str, n: int, timeout: float = 15) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        prog = progress(task_id)
        if prog["success"] >= n:
            return prog
        time.sleep(0.1)
    raise TimeoutError(f"task {task_id} success never reached {n}")


def main() -> int:
    print(f"== 1. 重置模拟站，设置 {DETAIL_DELAY}s 详情延迟 + DET-005 首次失败 ==")
    call("POST", f"{MOCK}/control/reset")
    call("POST", f"{MOCK}/control/delay?seconds={DETAIL_DELAY}")
    call("POST", f"{MOCK}/control/first-fail?detail_id=DET-005")

    print("== 2. 启动采集任务，等待详情开始入库（此时有 2 个在途慢请求）==")
    created = call("POST", f"{COLLECTOR}/api/tasks/start")
    task_id = created["task_id"]
    print(f"   task_id={task_id} run_id={created['run_id'][:8]}...")
    wait_success_at_least(task_id, 2)

    print("== 3. 第 1 次暂停：不等慢请求结束，暂停后旧请求落地也不得写入 ==")
    paused = call("POST", f"{COLLECTOR}/api/tasks/{task_id}/pause")
    snap = progress(task_id)
    print(f"   暂停瞬间: success={snap['success']} inflight={snap['inflight']}")
    assert 1 <= snap["inflight"] <= 2, "暂停瞬间进度应仍计入旧轮次在途请求"
    time.sleep(DETAIL_DELAY + 0.7)  # 等上一轮在途请求全部落地
    later = progress(task_id)
    print(f"   旧请求落地后: success={later['success']} inflight={later['inflight']}")
    assert later["success"] == snap["success"], "暂停后发生了迟到写入！"
    assert later["inflight"] == 0, "旧请求结束后名额应已释放"
    assert later["status"] == "paused"

    print("== 4. 恢复后连续 暂停 -> 立即恢复（旧请求仍在途，名额不得超额）==")
    resumed = call("POST", f"{COLLECTOR}/api/tasks/{task_id}/resume")
    assert resumed["run_id"] != paused["run_id"], "恢复必须生成新一代 run_id"
    for cycle in (2, 3):
        time.sleep(0.15)  # 让新一轮开始争抢名额
        prog = progress(task_id)
        print(f"   第 {cycle} 轮恢复后: inflight={prog['inflight']} success={prog['success']}")
        assert prog["inflight"] <= 2, "恢复后在途数（含旧轮次）超过 2！"

        paused = call("POST", f"{COLLECTOR}/api/tasks/{task_id}/pause")
        snap = progress(task_id)
        print(f"   第 {cycle} 次暂停瞬间: inflight={snap['inflight']} status={snap['status']}")
        assert 1 <= snap["inflight"] <= 2, "暂停瞬间进度应仍计入在途请求"
        # 紧接着立即恢复：旧请求还没结束，新一轮只能等名额空出
        resumed = call("POST", f"{COLLECTOR}/api/tasks/{task_id}/resume")
        prog = progress(task_id)
        assert prog["inflight"] <= 2, "立即恢复后在途数超过 2！"
        print(f"   立即恢复受理: inflight={prog['inflight']}（旧请求仍占名额）")

    print("== 5. 并发恢复必须被拒绝（同任务只启动一轮）==")
    dup = urllib.request.Request(f"{COLLECTOR}/api/tasks/{task_id}/resume", method="POST")
    try:
        urllib.request.urlopen(dup, timeout=10)
        raise AssertionError("并发恢复未被拒绝！")
    except urllib.error.HTTPError as exc:
        assert exc.code == 409, f"并发恢复应返回 409，实际 {exc.code}"
        print("   并发恢复返回 409，符合预期")

    print("== 6. 等待本轮收尾；若首次失败项被记为 failed，恢复补采直到完成 ==")
    final = None
    for attempt in range(1, 6):
        final = wait_status(task_id, {"completed", "failed"}, timeout=90)
        print(f"   第 {attempt} 次收尾: status={final['status']} "
              f"success={final['success']} failed={final['failed']}")
        if final["status"] == "completed":
            break
        # 失败留待重试：恢复只补采未成功项，已成功记录不重采
        call("POST", f"{COLLECTOR}/api/tasks/{task_id}/resume")
    else:
        raise AssertionError("多次恢复补采后任务仍未完成")
    assert final["success"] == 35, "去重后应有 35 条唯一许可"
    assert final["failed"] == 0 and final["pending"] == 0

    print("== 7. 校验记录字段与去重 ==")
    records = call("GET", f"{COLLECTOR}/api/tasks/{task_id}/records?limit=100")
    assert records["total"] == 35
    permit_nos = [r["permit_no"] for r in records["items"]]
    assert len(set(permit_nos)) == 35, "存在重复许可号"
    for r in records["items"]:
        assert r["project_name"] and r["address"] and r["issue_date"], f"字段缺失: {r}"
    dup_entry = next(r for r in records["items"] if r["permit_no"] == "PERMIT-004")
    print(f"   跨页重复许可 {dup_entry['permit_no']} 仅一条，来源页={dup_entry['source_pages']}")

    mock_state = call("GET", f"{MOCK}/control/state")
    print(f"   模拟站统计: 详情请求={mock_state['detail_requests']} "
          f"峰值在途={mock_state['max_inflight']} 模拟失败次数={len(mock_state['failed_attempts'])}")
    assert mock_state["max_inflight"] <= 2, "连续暂停/恢复期间超出每任务 2 个在途请求的上限"
    assert len(mock_state["failed_attempts"]) == 1, "首次失败应恰好发生一次"

    print("\n全部演示断言通过 ✔")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"\n演示失败: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)
