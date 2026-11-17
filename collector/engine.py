"""采集引擎：任务生命周期、并发控制、暂停/恢复语义。

关键设计
========
1. 代际令牌 run_id
   每次启动/恢复都会生成新的 run_id 并写入 tasks.run_id；暂停时同样换新 run_id。
   采集轮次中**任何写库动作**都先校验内存中的 run_id 是否仍是当前代际，
   任务行更新还带 run_id CAS。暂停响应返回后旧轮次立刻失去写权限，
   即使暂停后“随即恢复”，恢复产生的是另一代 run_id，上一轮的迟到响应
   既改不了 permit_records 也改不了 tasks 进度。

2. 每任务最多 2 个在途请求
   每轮采集持有独立 asyncio.Semaphore(MAX_INFLIGHT)，所有 HTTP 请求
   （目录页 + 详情）都先获取信号量；inflight 计数也在信号量保护区内维护。

3. 同任务并发恢复只能启动一轮
   每任务一把 asyncio.Lock（resume_locks），start/resume 临界区串行；
   运行中的任务再次 start/resume 直接返回 409。

4. 失败留待重试 / 重启恢复
   详情失败仅把记录置 failed 并记 last_error，不阻塞其他详情；
   下一轮（恢复或进程重启后）只查 status != 'success' 的记录补采。
   全部唯一许可都 success 才置 completed。

5. 去重
   permit_records 以 (task_id, permit_no) 为复合主键，目录里跨页/跨
   detail_id 的重复条目在 upsert_catalog_permit 合并，同一任务内每个
   许可最终只有一条记录，不同任务互不干扰。
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any, Optional

import httpx

from . import config
from .db import db


class TaskControl:
    """一轮采集的运行期控制块；暂停时从注册表摘除并换代 run_id。"""

    def __init__(self, task_id: str, run_id: str) -> None:
        self.task_id = task_id
        self.run_id = run_id
        self.semaphore = asyncio.Semaphore(config.MAX_INFLIGHT)
        self.inflight = 0


class CollectorEngine:
    def __init__(self) -> None:
        # task_id -> 当前代际控制块
        self._controls: dict[str, TaskControl] = {}
        # 每任务的启动/恢复互斥锁（按事件循环懒创建，见 _lock_for）
        self._resume_locks: dict[str, asyncio.Lock] = {}
        # 串行化所有 SQLite 写操作（配合 run_id 检查，消除检查-写入竞态）
        self._write_lock: Optional[asyncio.Lock] = None
        self._registry_lock: Optional[asyncio.Lock] = None
        self._client: Optional[httpx.AsyncClient] = None
        self._client_loop: Optional[asyncio.AbstractEventLoop] = None

    async def startup(self) -> None:
        # 锁与客户端都按事件循环懒创建：进程内/测试中跨循环复用时自动重建，
        # 避免“对象绑定旧循环、新循环里 RuntimeError/请求挂死”
        loop = asyncio.get_running_loop()
        self._write_lock = asyncio.Lock()
        self._registry_lock = asyncio.Lock()
        self._resume_locks.clear()
        self._controls.clear()
        self._client = None
        self._client_loop = loop
        # 进程重启：未正常结束的 running 任务标 interrupted，等待人工恢复
        await db.mark_interrupted_on_startup()

    async def shutdown(self) -> None:
        if self._client is not None:
            await self._client.aclose()
        self._client = None
        self._client_loop = None
        self._write_lock = None
        self._registry_lock = None
        self._resume_locks.clear()
        self._controls.clear()

    def _get_client(self) -> httpx.AsyncClient:
        loop = asyncio.get_running_loop()
        if self._client is None or self._client_loop is not loop:
            self._client = httpx.AsyncClient(timeout=config.HTTP_TIMEOUT)
            self._client_loop = loop
        return self._client

    # ---------------------------------------------------------- 内部工具

    def _lock_for(self, task_id: str) -> asyncio.Lock:
        if task_id not in self._resume_locks:
            self._resume_locks[task_id] = asyncio.Lock()
        return self._resume_locks[task_id]

    @property
    def write_lock(self) -> asyncio.Lock:
        assert self._write_lock is not None, "engine not started"
        return self._write_lock

    @property
    def registry_lock(self) -> asyncio.Lock:
        assert self._registry_lock is not None, "engine not started"
        return self._registry_lock

    async def _current_control(self, task_id: str) -> Optional[TaskControl]:
        async with self.registry_lock:
            return self._controls.get(task_id)

    async def _is_current(self, task_id: str, run_id: str) -> bool:
        """代际校验：控制块仍在注册表且 run_id 未换代。

        暂停会摘除控制块并换代，恢复会建立新控制块——两种情况下旧轮次
        的校验结果都是 False，从而拒绝一切迟到写入。
        """
        async with self.registry_lock:
            control = self._controls.get(task_id)
            return control is not None and control.run_id == run_id

    async def _http_get_json(
        self, control: TaskControl, url: str, params: Optional[dict] = None
    ) -> Any:
        """带每任务在途上限的 GET JSON，在途请求任何时刻不超过 MAX_INFLIGHT。"""
        client = self._get_client()
        async with control.semaphore:
            control.inflight += 1
            try:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                return resp.json()
            finally:
                control.inflight -= 1

    # ---------------------------------------------------------- 启动/暂停/恢复

    async def start_task(self, target_url: Optional[str]) -> dict[str, Any]:
        task_id = uuid.uuid4().hex
        async with self._lock_for(task_id):
            run_id = uuid.uuid4().hex
            await db.insert_task(
                {
                    "task_id": task_id,
                    "target_url": target_url or config.MOCK_BASE_URL,
                    "status": "running",
                    "run_id": run_id,
                    "catalog_pages_done": 0,
                    "catalog_total_pages": None,
                    "last_error": None,
                    "started_at": sqlite_now(),
                    "updated_at": sqlite_now(),
                }
            )
            control = TaskControl(task_id, run_id)
            async with self.registry_lock:
                self._controls[task_id] = control
            asyncio.create_task(self._run_round(task_id, run_id))
            return {"task_id": task_id, "status": "running", "run_id": run_id}

    async def pause_task(self, task_id: str) -> dict[str, Any]:
        task = await db.get_task(task_id)
        if task is None:
            raise KeyError(task_id)
        if task["status"] != "running":
            raise ValueError(f"task is {task['status']}, only a running task can pause")

        new_run_id = uuid.uuid4().hex
        async with self.write_lock:
            # 先换代再置 paused：本响应一旦返回，旧轮次的代际校验全部失败
            ok = await db.update_task(
                task_id,
                {
                    "status": "paused",
                    "run_id": new_run_id,
                    "updated_at": sqlite_now(),
                },
                expected_run_id=task["run_id"],
            )
            if not ok:
                raise RuntimeError("pause conflict: task run changed concurrently")
            async with self.registry_lock:
                control = self._controls.pop(task_id, None)
            if control is not None:
                # 旧控制块立即失效；在途协程仍持有它完成 HTTP 调用，
                # 但写库前的 _is_current 检查不会再通过
                control.run_id = new_run_id

        return {
            "task_id": task_id,
            "status": "paused",
            "run_id": new_run_id,
            "message": "paused; late responses from the previous round cannot write",
        }

    async def resume_task(self, task_id: str) -> dict[str, Any]:
        task = await db.get_task(task_id)
        if task is None:
            raise KeyError(task_id)
        if task["status"] == "completed":
            raise ValueError("task already completed")

        # 同任务并发恢复：锁内检查是否已有在跑轮次，保证只启动一轮
        async with self._lock_for(task_id):
            if await self._current_control(task_id) is not None:
                raise RuntimeError("task is already running")

            run_id = uuid.uuid4().hex
            async with self.write_lock:
                await db.update_task(
                    task_id,
                    {
                        "status": "running",
                        "run_id": run_id,
                        "last_error": None,
                        "updated_at": sqlite_now(),
                    },
                )
            control = TaskControl(task_id, run_id)
            async with self.registry_lock:
                self._controls[task_id] = control
            asyncio.create_task(self._run_round(task_id, run_id))
            return {"task_id": task_id, "status": "running", "run_id": run_id}

    # ---------------------------------------------------------- 查询

    async def get_progress(self, task_id: str) -> dict[str, Any]:
        task = await db.get_task(task_id)
        if task is None:
            raise KeyError(task_id)
        total = await db.count_permits(task_id)
        success = await db.count_permits(task_id, "success")
        failed = await db.count_permits(task_id, "failed")
        pending = await db.count_permits(task_id, "pending")
        control = await self._current_control(task_id)
        inflight = control.inflight if control else 0
        return {
            "task_id": task_id,
            "status": task["status"],
            "target_url": task["target_url"],
            "run_id": task["run_id"],
            "total_unique_permits": total,
            "success": success,
            "failed": failed,
            "pending": pending,
            "inflight": inflight,
            "done": task["status"] == "completed",
            "last_error": task["last_error"],
            "catalog_pages_done": task["catalog_pages_done"],
            "catalog_total_pages": task["catalog_total_pages"],
            "started_at": task["started_at"],
            "updated_at": task["updated_at"],
        }

    # ---------------------------------------------------------- 采集轮次

    async def _run_round(self, task_id: str, run_id: str) -> None:
        """一轮完整采集：拉目录（已完成页跳过）-> 并发补采未成功详情。"""
        task = await db.get_task(task_id)
        base_url = task["target_url"].rstrip("/")
        try:
            await self._collect_catalog(task_id, run_id, base_url)
            if not await self._is_current(task_id, run_id):
                return
            await self._collect_details(task_id, run_id, base_url)
            if not await self._is_current(task_id, run_id):
                return

            # 收尾：只有仍存活的当前轮次可以写终态
            unfinished = await db.list_unfinished_permits(task_id)
            async with self.write_lock:
                if not await self._is_current(task_id, run_id):
                    return
                if unfinished:
                    await db.update_task(
                        task_id,
                        {
                            "status": "failed",
                            "last_error": f"{len(unfinished)} permit(s) not collected",
                            "updated_at": sqlite_now(),
                        },
                        expected_run_id=run_id,
                    )
                else:
                    await db.update_task(
                        task_id,
                        {"status": "completed", "last_error": None, "updated_at": sqlite_now()},
                        expected_run_id=run_id,
                    )
        except Exception as exc:  # 目录持续失败等：任务置 failed，已入库记录保留，可恢复
            async with self.write_lock:
                if await self._is_current(task_id, run_id):
                    await db.update_task(
                        task_id,
                        {"status": "failed", "last_error": str(exc), "updated_at": sqlite_now()},
                        expected_run_id=run_id,
                    )
        finally:
            async with self.registry_lock:
                control = self._controls.get(task_id)
                if control is not None and control.run_id == run_id:
                    self._controls.pop(task_id, None)

    async def _collect_catalog(self, task_id: str, run_id: str, base_url: str) -> None:
        """逐页拉目录并入库；已完成的页跳过（重启/恢复只补未成功项）。"""
        task = await db.get_task(task_id)
        done_pages = task["catalog_pages_done"] or 0
        total_pages: Optional[int] = task["catalog_total_pages"]
        # 目录此前已完整抓完（含进程重启后恢复）：直接跳过，不再请求越界页
        if total_pages is not None and done_pages >= total_pages:
            return
        control = await self._current_control(task_id)
        assert control is not None
        page = done_pages + 1
        while True:
            if not await self._is_current(task_id, run_id):
                return

            payload = None
            last_error: Optional[Exception] = None
            for attempt in range(config.CATALOG_MAX_ATTEMPTS):
                try:
                    payload = await self._http_get_json(
                        control, f"{base_url}/api/permits", params={"page": page}
                    )
                    break
                except Exception as exc:  # noqa: BLE001 - 重试后上抛
                    last_error = exc
                    await asyncio.sleep(0.05 * (attempt + 1))
            if payload is None:
                raise RuntimeError(f"catalog page {page} failed: {last_error}")

            total_pages = payload["total_pages"]
            async with self.write_lock:
                if not await self._is_current(task_id, run_id):
                    return
                for item in payload["items"]:
                    await db.upsert_catalog_permit(
                        task_id, item["permit_no"], item["detail_id"], page
                    )
                await db.update_task(
                    task_id,
                    {
                        "catalog_pages_done": page,
                        "catalog_total_pages": total_pages,
                        "updated_at": sqlite_now(),
                    },
                    expected_run_id=run_id,
                )

            if page >= total_pages:
                return
            page += 1

    async def _collect_details(self, task_id: str, run_id: str, base_url: str) -> None:
        """并发采集详情，每任务在途请求受信号量约束（默认上限 2）。"""
        control = await self._current_control(task_id)
        assert control is not None
        unfinished = await db.list_unfinished_permits(task_id)
        if not unfinished:
            return

        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        for row in unfinished:
            queue.put_nowait(row)

        async def worker() -> None:
            while True:
                if not await self._is_current(task_id, run_id):
                    return
                try:
                    row = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                await self._fetch_one_detail(task_id, run_id, control, base_url, row)
                queue.task_done()

        # 4 个 worker 抢 2 个信号量名额：并发度真实受 MAX_INFLIGHT 约束
        workers = [asyncio.create_task(worker()) for _ in range(4)]
        await asyncio.gather(*workers)

    async def _fetch_one_detail(
        self,
        task_id: str,
        run_id: str,
        control: TaskControl,
        base_url: str,
        row: dict[str, Any],
    ) -> None:
        permit_no = row["permit_no"]
        detail_id = row["source_detail_id"]
        url = f"{base_url}/api/permits/details/{detail_id}"

        payload: Optional[dict[str, Any]] = None
        last_error = "unknown error"
        for _attempt in range(config.DETAIL_MAX_ATTEMPTS):
            if not await self._is_current(task_id, run_id):
                return  # 已暂停/换代：迟到响应一律丢弃，不写库
            try:
                payload = await self._http_get_json(control, url)
                break
            except httpx.HTTPStatusError as exc:
                last_error = f"HTTP {exc.response.status_code}"
            except httpx.HTTPError as exc:
                last_error = f"{type(exc).__name__}: {exc}"

        # 写库前再次代际校验，并在全局写锁内完成“检查 + 写入”
        async with self.write_lock:
            if not await self._is_current(task_id, run_id):
                return  # 暂停响应已返回 / 已恢复为新一轮：拒绝迟到写入
            if payload is not None:
                if payload.get("permit_no") != permit_no:
                    await db.mark_permit_attempt(
                        task_id,
                        permit_no,
                        f"permit_no mismatch: got {payload.get('permit_no')}",
                    )
                else:
                    await db.mark_permit_success(task_id, permit_no, payload)
            else:
                # 失败留待重试：状态 failed，下一轮恢复只补采未成功项
                await db.mark_permit_attempt(task_id, permit_no, last_error)


def sqlite_now() -> str:
    """与 SQLite datetime('now') 风格一致的 UTC 时间字符串。"""
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


engine = CollectorEngine()
