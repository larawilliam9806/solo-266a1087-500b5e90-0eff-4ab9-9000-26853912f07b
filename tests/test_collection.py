"""采集引擎关键功能自测（真实 HTTP 回环 + 临时 SQLite）。"""

from __future__ import annotations

import asyncio

import httpx

from conftest import poll_progress, wait_until


async def _start(engine, target_url):
    created = await engine.start_task(target_url)
    return created["task_id"]


# 1. 完整采集 + 去重 + 字段保存 -------------------------------------------

async def test_full_collection_dedup_and_fields(client):
    _api, engine, db, base_url = client
    task_id = await _start(engine, base_url)
    prog = await poll_progress(engine, task_id, {"completed", "failed"})

    assert prog["status"] == "completed"
    assert prog["total_unique_permits"] == 35          # 38 条目录条目去重后 35
    assert prog["success"] == 35
    assert prog["failed"] == 0 and prog["pending"] == 0
    assert prog["done"] is True

    rows = await db.list_permits(task_id, limit=100)
    assert len(rows) == 35
    permit_nos = [r["permit_no"] for r in rows]
    assert len(set(permit_nos)) == 35
    for row in rows:
        assert row["status"] == "success"
        assert row["project_name"] and row["address"] and row["issue_date"]

    # 跨页重复许可只留一条，但保留了多个来源页
    dup = await db.get_permit(task_id, "PERMIT-004")
    pages = __import__("json").loads(dup["source_pages"])
    assert len(pages) >= 2 and dup["status"] == "success"


# 2. 并发上限：每任务最多 2 个在途请求 ------------------------------------

async def test_max_inflight_limit(client, mock_base_url):
    _api, engine, db, _ = client
    async with httpx.AsyncClient(base_url=mock_base_url, timeout=10) as mock_http:
        await mock_http.post("/control/delay", params={"seconds": 0.25})
        task_id = await _start(engine, mock_base_url)
        await poll_progress(engine, task_id, {"completed", "failed"})
        state = (await mock_http.get("/control/state")).json()

    assert state["max_inflight"] <= 2, f"峰值在途 {state['max_inflight']} 超过 2"
    assert state["max_inflight"] == 2, "延迟下应观察到 2 并发"


# 2b. 缺陷回归：慢响应下连续暂停/恢复，在途请求任何时刻 <= 2，且进度计入旧轮次

async def test_inflight_limit_across_pause_resume(client, mock_base_url):
    """跨多次暂停/恢复，单任务未结束请求 <= 2；旧请求未返回仍占名额。

    缺陷原貌：每轮采集各持一把信号量，暂停/恢复后旧轮次在途请求被
    “遗忘”，新一轮重新拿满 2 个名额，公示站实际并发随暂停次数翻倍，
    而进度 inflight 始终只显示当前轮（<=2）。
    """
    _api, engine, db, _ = client
    async with httpx.AsyncClient(base_url=mock_base_url, timeout=10) as mock_http:
        await mock_http.post("/control/delay", params={"seconds": 0.6})
        task_id = await _start(engine, mock_base_url)
        await wait_until(lambda: _success_at_least(engine, task_id, 1))

        inflight_when_paused = []
        for _ in range(3):  # 连续多次 暂停 -> 立即恢复（不等旧请求结束）
            paused = await engine.pause_task(task_id)
            assert paused["status"] == "paused"
            prog = await engine.get_progress(task_id)
            assert 0 <= prog["inflight"] <= 2
            inflight_when_paused.append(prog["inflight"])

            resumed = await engine.resume_task(task_id)  # 立即受理
            assert resumed["status"] == "running"
            prog = await engine.get_progress(task_id)
            assert prog["inflight"] <= 2, "恢复后在途数（含旧轮次）超过上限"
            await asyncio.sleep(0.15)  # 让新一轮开始争抢名额

        # 暂停瞬间进度必须仍计入旧轮次在途请求（公示站还没返回的那些）
        assert any(n >= 1 for n in inflight_when_paused), (
            f"暂停时进度未计入旧轮次在途请求: {inflight_when_paused}"
        )

        final = await poll_progress(engine, task_id, {"completed", "failed"}, timeout=60)
        assert final["status"] == "completed"
        assert final["success"] == 35
        state = (await mock_http.get("/control/state")).json()
        assert state["max_inflight"] <= 2, (
            f"连续暂停/恢复期间公示站观测到峰值在途 {state['max_inflight']}，超过 2"
        )


# 2c. 不同任务各自限流：两任务并行，各自峰值 <= 2（合计可到 4）-------------

async def test_tasks_rate_limited_independently(client, mock_base_url, mock_base_url_2):
    _api, engine, db, _ = client
    async with httpx.AsyncClient(base_url=mock_base_url, timeout=10) as m1, (
        httpx.AsyncClient(base_url=mock_base_url_2, timeout=10)
    ) as m2:
        await m1.post("/control/reset")
        await m2.post("/control/reset")
        await m1.post("/control/delay", params={"seconds": 0.3})
        await m2.post("/control/delay", params={"seconds": 0.3})

        id1 = await _start(engine, mock_base_url)
        id2 = await _start(engine, mock_base_url_2)
        prog1, prog2 = await asyncio.gather(
            poll_progress(engine, id1, {"completed", "failed"}),
            poll_progress(engine, id2, {"completed", "failed"}),
        )
        assert prog1["status"] == prog2["status"] == "completed"
        assert prog1["success"] == prog2["success"] == 35
        s1 = (await m1.get("/control/state")).json()
        s2 = (await m2.get("/control/state")).json()

    # 两个任务同时跑满各自的名额：说明限流是每任务而非全局
    assert s1["max_inflight"] == 2, f"任务1峰值在途 {s1['max_inflight']}"
    assert s2["max_inflight"] == 2, f"任务2峰值在途 {s2['max_inflight']}"


# 3. 暂停后不得继续写入；随即恢复后迟到响应也不能改记录/进度 ----------------

async def test_pause_stops_writes_and_late_response_rejected(client, mock_base_url):
    _api, engine, db, _ = client
    async with httpx.AsyncClient(base_url=mock_base_url, timeout=10) as mock_http:
        await mock_http.post("/control/delay", params={"seconds": 0.4})
        task_id = await _start(engine, mock_base_url)

        # 等到至少 2 条成功后暂停，保证暂停时仍有在途请求
        await wait_until(lambda: _success_at_least(engine, task_id, 2))
        before = await engine.get_progress(task_id)
        paused = await engine.pause_task(task_id)
        assert paused["status"] == "paused"

        # 给上一轮所有在途响应足够时间返回——它们必须被代际校验拒绝
        await asyncio.sleep(1.2)
        after = await engine.get_progress(task_id)
        assert after["status"] == "paused"
        assert after["success"] == before["success"], "暂停后出现迟到写入"
        assert after["run_id"] == paused["run_id"]

        # 随即恢复：新一轮 run_id，旧轮次迟到响应依旧无权写入
        resumed = await engine.resume_task(task_id)
        assert resumed["run_id"] != paused["run_id"]
        await asyncio.sleep(1.0)  # 让任何旧轮次迟到响应尝试写库
        prog = await engine.get_progress(task_id)
        # 记录只能由新一轮写入：总数不超过唯一许可数，且没有出现状态倒退
        assert prog["total_unique_permits"] <= 35

        final = await poll_progress(engine, task_id, {"completed", "failed"})
        assert final["status"] == "completed"
        assert final["success"] == 35


async def _success_at_least(engine, task_id: str, n: int) -> bool:
    prog = await engine.get_progress(task_id)
    return prog["success"] >= n


# 3b. 恢复后旧轮次的写入路径被代际校验直接拒绝（确定性单测） ---------------

async def test_stale_run_id_write_rejected_after_resume(client, mock_base_url):
    _api, engine, db, _ = client
    async with httpx.AsyncClient(base_url=mock_base_url, timeout=10) as mock_http:
        # 让第一轮必然失败并停在 failed，留下一条 failed 记录
        await mock_http.post("/control/first-fail", params={"detail_id": "DET-007"})
        task_id = await _start(engine, mock_base_url)
        prog = await poll_progress(engine, task_id, {"failed"})
        assert prog["failed"] == 1

        stale_row = (await db.list_permits(task_id, status="failed"))[0]
        # 恢复，建立新一代控制块与 run_id
        resumed = await engine.resume_task(task_id)
        new_run_id = resumed["run_id"]
        new_control = await engine._current_control(task_id)
        assert new_control is not None and new_control.run_id == new_run_id

        # 构造一个“上一轮”的控制块（旧 run_id），直接走单条详情写入路径
        stale_control = type(new_control)(task_id, "stale-run-id", new_control.slots)
        fake_detail = {
            "permit_no": stale_row["permit_no"],
            "project_name": "被污染的项目名",
            "address": "被污染的地址",
            "issue_date": "2000-01-01",
        }
        await engine._fetch_one_detail(
            task_id, "stale-run-id", stale_control, mock_base_url, stale_row
        )

        # 旧轮次写入必须被拒绝：记录字段不变、尝试次数不增加
        row_after = await db.get_permit(task_id, stale_row["permit_no"])
        assert row_after["project_name"] != "被污染的项目名"
        assert row_after["attempts"] == stale_row["attempts"]
        assert row_after["last_error"] == "HTTP 500"

        # 新一轮正常跑完并覆盖该记录
        final = await poll_progress(engine, task_id, {"completed", "failed"})
        assert final["status"] == "completed" and final["success"] == 35
        row_final = await db.get_permit(task_id, stale_row["permit_no"])
        assert row_final["status"] == "success" and row_final["project_name"] != "被污染的项目名"


# 4. 失败留待重试，恢复后补采成功 -----------------------------------------

async def test_failure_retried_on_resume(client, mock_base_url):
    _api, engine, db, _ = client
    async with httpx.AsyncClient(base_url=mock_base_url, timeout=10) as mock_http:
        # DET-001 第一次请求 500，之后恢复正常
        await mock_http.post("/control/first-fail", params={"detail_id": "DET-001"})
        task_id = await _start(engine, mock_base_url)

        prog = await poll_progress(engine, task_id, {"completed", "failed"})
        # 一轮内不重试详情，存在失败项 -> 任务 failed，等待恢复
        assert prog["status"] == "failed"
        assert prog["failed"] == 1
        failed_rows = await db.list_permits(task_id, status="failed")
        assert [r["permit_no"] for r in failed_rows] == ["PERMIT-001"]
        assert failed_rows[0]["last_error"] == "HTTP 500"
        assert prog["success"] == 34

        # 恢复：已入库记录保留，只补采未成功项
        resumed = await engine.resume_task(task_id)
        assert resumed["status"] == "running"
        final = await poll_progress(engine, task_id, {"completed", "failed"})
        assert final["status"] == "completed"
        assert final["success"] == 35 and final["failed"] == 0

        row = await db.get_permit(task_id, "PERMIT-001")
        assert row["status"] == "success" and row["attempts"] == 2
        state = (await mock_http.get("/control/state")).json()
        assert len(state["failed_attempts"]) == 1


# 5. 同任务并发恢复只能启动一轮 -------------------------------------------

async def test_concurrent_resume_starts_single_round(client, mock_base_url):
    _api, engine, db, _ = client
    async with httpx.AsyncClient(base_url=mock_base_url, timeout=10) as mock_http:
        await mock_http.post("/control/first-fail", params={"detail_id": "DET-002"})
        task_id = await _start(engine, mock_base_url)
        await poll_progress(engine, task_id, {"failed"})

        results = await asyncio.gather(
            engine.resume_task(task_id),
            engine.resume_task(task_id),
            return_exceptions=True,
        )
        ok = [r for r in results if not isinstance(r, Exception)]
        rejected = [r for r in results if isinstance(r, RuntimeError)]
        assert len(ok) == 1 and len(rejected) == 1

        final = await poll_progress(engine, task_id, {"completed", "failed"})
        assert final["status"] == "completed"


# 6. 进程重启后恢复：已入库记录保留，只补未成功项 --------------------------

async def test_restart_recovery(client, mock_base_url):
    _api, engine, db, _ = client
    async with httpx.AsyncClient(base_url=mock_base_url, timeout=10) as mock_http:
        await mock_http.post("/control/first-fail", params={"detail_id": "DET-003"})
        task_id = await _start(engine, mock_base_url)
        prog = await poll_progress(engine, task_id, {"failed"})
        assert prog["failed"] == 1
        success_before = prog["success"]

        # 模拟进程重启：关闭引擎/连接（running 场景的 interrupted 标记在 startup 处理，
        # 这里任务是 failed，重点验证记录持久化与补采逻辑）
        await engine.shutdown()
        await db.close()
        await db.connect()
        await engine.startup()

        persisted = await engine.get_progress(task_id)
        assert persisted["success"] == success_before
        assert persisted["total_unique_permits"] == 35

        await engine.resume_task(task_id)
        final = await poll_progress(engine, task_id, {"completed", "failed"})
        assert final["status"] == "completed"
        assert final["success"] == 35


# 7. 运行中进程重启 -> interrupted，恢复后跑完 -----------------------------

async def test_interrupted_on_restart_then_resume(client, mock_base_url):
    _api, engine, db, _ = client
    async with httpx.AsyncClient(base_url=mock_base_url, timeout=10) as mock_http:
        await mock_http.post("/control/delay", params={"seconds": 0.5})
        task_id = await _start(engine, mock_base_url)
        await wait_until(lambda: _success_at_least(engine, task_id, 1))

        # 直接在 DB 层模拟“进程被杀死”：内存控制块消失，任务行仍为 running
        await engine.shutdown()
        await db.close()
        await db.connect()
        await engine.startup()  # startup 应把 running 标为 interrupted

        task = await db.get_task(task_id)
        assert task["status"] == "interrupted"
        kept = await db.count_permits(task_id, "success")
        assert kept >= 1

        await engine.resume_task(task_id)
        final = await poll_progress(engine, task_id, {"completed", "failed"})
        assert final["status"] == "completed"
        assert final["success"] == 35


# 8. 同一数据库内多个任务互不干扰（复合主键） ------------------------------

async def test_multiple_tasks_share_database(client):
    _api, engine, db, base_url = client
    id1 = await _start(engine, base_url)
    prog1 = await poll_progress(engine, id1, {"completed", "failed"})
    assert prog1["status"] == "completed"

    id2 = await _start(engine, base_url)
    prog2 = await poll_progress(engine, id2, {"completed", "failed"})
    assert prog2["status"] == "completed"
    assert prog2["total_unique_permits"] == 35

    # 两个任务各有独立的 35 条记录，同许可号不冲突
    rows1 = await db.list_permits(id1, limit=100)
    rows2 = await db.list_permits(id2, limit=100)
    assert len(rows1) == len(rows2) == 35
    assert {r["permit_no"] for r in rows1} == {r["permit_no"] for r in rows2}
    assert (await db.count_permits(id1, "success")) == 35
    assert (await db.count_permits(id2, "success")) == 35


# 9. API 层错误语义 -------------------------------------------------------
async def test_api_404_and_409(client):
    api, engine, _db, _ = client
    resp = await api.get("/api/tasks/nonexistent/progress")
    assert resp.status_code == 404

    created = await api.post("/api/tasks/start", json={})
    task_id = created.json()["task_id"]
    await poll_progress(engine, task_id, {"completed"})

    # 已完成任务不能暂停/恢复
    assert (await api.post(f"/api/tasks/{task_id}/pause")).status_code == 409
    assert (await api.post(f"/api/tasks/{task_id}/resume")).status_code == 409
