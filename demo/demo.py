#!/usr/bin/env python3
"""端到端演示脚本（仅用标准库，容器/本地均可运行）。

流程：
1. 重置模拟站，设置详情延迟与“首次失败”
2. 启动采集任务，轮询进度，演示暂停 -> 恢复
3. 验证暂停后没有迟到写入、恢复后只补采未成功项
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


def wait_status(task_id: str, acceptable: set[str], timeout: float = 30) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        prog = call("GET", f"{COLLECTOR}/api/tasks/{task_id}/progress")
        if prog["status"] in acceptable:
            return prog
        time.sleep(0.2)
    raise TimeoutError(f"task {task_id} never reached {acceptable}")


def main() -> int:
    print("== 1. 重置模拟站，设置 0.4s 详情延迟 + DET-005 首次失败 ==")
    call("POST", f"{MOCK}/control/reset")
    call("POST", f"{MOCK}/control/delay?seconds=0.4")
    call("POST", f"{MOCK}/control/first-fail?detail_id=DET-005")

    print("== 2. 启动采集任务 ==")
    created = call("POST", f"{COLLECTOR}/api/tasks/start")
    task_id = created["task_id"]
    print(f"   task_id={task_id} run_id={created['run_id'][:8]}...")

    # 等目录抓完、详情开始入库后暂停
    prog = wait_status(task_id, {"running"})
    deadline = time.time() + 10
    while time.time() < deadline:
        prog = call("GET", f"{COLLECTOR}/api/tasks/{task_id}/progress")
        if prog["success"] >= 3:
            break
        time.sleep(0.1)

    print("== 3. 暂停 ==")
    paused = call("POST", f"{COLLECTOR}/api/tasks/{task_id}/pause")
    print(f"   pause 响应: status={paused['status']} 新 run_id={paused['run_id'][:8]}...")
    snapshot = call("GET", f"{COLLECTOR}/api/tasks/{task_id}/progress")
    print(f"   暂停瞬间: success={snapshot['success']} inflight={snapshot['inflight']}")

    # 等待上一轮在途请求全部落地（应被代际校验拒绝，记录数不再变化）
    time.sleep(1.5)
    later = call("GET", f"{COLLECTOR}/api/tasks/{task_id}/progress")
    print(f"   等待 1.5s 后: success={later['success']}（须与暂停瞬间一致）")
    assert later["success"] == snapshot["success"], "暂停后发生了迟到写入！"
    assert later["status"] == "paused"

    print("== 4. 随即恢复（模拟站仍有延迟；旧轮次迟到响应不得污染新一轮）==")
    resumed = call("POST", f"{COLLECTOR}/api/tasks/{task_id}/resume")
    print(f"   resume 响应: run_id={resumed['run_id'][:8]}...")
    assert resumed["run_id"] != paused["run_id"], "恢复必须生成新一代 run_id"

    # 并发恢复：第二个请求必须被拒绝，同任务只能有一轮在跑
    dup = urllib.request.Request(
        f"{COLLECTOR}/api/tasks/{task_id}/resume", method="POST"
    )
    try:
        urllib.request.urlopen(dup, timeout=10)
        raise AssertionError("并发恢复未被拒绝！")
    except urllib.error.HTTPError as exc:
        assert exc.code == 409, f"并发恢复应返回 409，实际 {exc.code}"
        print("   并发恢复返回 409，符合预期（同任务只启动一轮）")

    print("== 5. 等待任务完成（首次失败项经恢复后补采成功）==")
    final = wait_status(task_id, {"completed", "failed"}, timeout=60)
    print(f"   最终状态: {final['status']} success={final['success']}/"
          f"{final['total_unique_permits']} failed={final['failed']}")
    assert final["status"] == "completed", f"任务未完成: {final}"
    assert final["success"] == 35, "去重后应有 35 条唯一许可"
    assert final["failed"] == 0 and final["pending"] == 0

    print("== 6. 校验记录字段与去重 ==")
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
    assert mock_state["max_inflight"] <= 2, "超出每任务 2 个在途请求的上限"
    assert len(mock_state["failed_attempts"]) == 1, "首次失败应恰好发生一次"

    print("\n全部演示断言通过 ✔")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"\n演示失败: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)
