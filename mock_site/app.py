"""本地模拟施工许可公示站。

提供固定分页目录、详情接口，以及可通过 API 控制的：
- 详情接口统一延迟（delay 秒）
- 首次请求失败（first_fail_detail_id，仅第一次访问该详情返回 500，
  之后自动清除，用来演示“失败留待重试，恢复后补采成功”）

无需访问任何外网。
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from . import data


class ControlState:
    """运行期控制状态（进程内存）。

    全部在 uvicorn 单事件循环内访问，无需加锁；计数器同步更新即可，
    避免锁把并发请求串行化而影响“在途请求”观测。
    """

    def __init__(self) -> None:
        self.delay: float = 0.0
        self.first_fail_detail_id: str | None = None
        self.inflight: int = 0
        self.max_inflight: int = 0
        self.catalog_requests: int = 0
        self.detail_requests: int = 0
        self.failed_attempts: list[dict] = []

    def enter_request(self) -> None:
        self.inflight += 1
        if self.inflight > self.max_inflight:
            self.max_inflight = self.inflight

    def leave_request(self) -> None:
        self.inflight -= 1

    def snapshot(self) -> dict:
        return {
            "delay": self.delay,
            "first_fail_detail_id": self.first_fail_detail_id,
            "inflight": self.inflight,
            "max_inflight": self.max_inflight,
            "catalog_requests": self.catalog_requests,
            "detail_requests": self.detail_requests,
            "failed_attempts": list(self.failed_attempts),
        }


state = ControlState()


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(title="模拟施工许可公示站", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get("/api/permits")
async def list_permits(page: int = 1) -> dict:
    state.enter_request()
    try:
        state.catalog_requests += 1
        result = data.catalog_page(page)
        if result is None:
            raise HTTPException(status_code=404, detail=f"page {page} not found")
        return result
    finally:
        state.leave_request()


@app.get("/api/permits/details/{detail_id}")
async def get_detail(detail_id: str) -> dict:
    state.enter_request()
    try:
        state.detail_requests += 1
        # 可控延迟，便于演示暂停时仍有在途请求的场景
        if state.delay:
            await asyncio.sleep(state.delay)

        # 首次失败：命中后只失败一次，随后自动恢复，等待采集端重试
        if state.first_fail_detail_id == detail_id:
            state.first_fail_detail_id = None
            attempt = {
                "detail_id": detail_id,
                "at": time.time(),
                "reason": "simulated first-attempt failure",
            }
            state.failed_attempts.append(attempt)
            raise HTTPException(status_code=500, detail=attempt["reason"])

        result = data.detail(detail_id)
        if result is None:
            raise HTTPException(status_code=404, detail=f"detail {detail_id} not found")
        return result
    finally:
        state.leave_request()


@app.post("/control/reset")
async def control_reset() -> dict:
    """重置所有控制项与统计计数器。"""
    state.delay = 0.0
    state.first_fail_detail_id = None
    state.inflight = 0
    state.max_inflight = 0
    state.catalog_requests = 0
    state.detail_requests = 0
    state.failed_attempts.clear()
    return {"status": "reset"}


@app.post("/control/delay")
async def control_delay(seconds: float) -> dict:
    if seconds < 0:
        raise HTTPException(status_code=400, detail="seconds must be >= 0")
    state.delay = seconds
    return {"delay": state.delay}


@app.post("/control/first-fail")
async def control_first_fail(detail_id: str) -> dict:
    if data.detail(detail_id) is None:
        raise HTTPException(status_code=404, detail=f"unknown detail_id {detail_id}")
    state.first_fail_detail_id = detail_id
    return {"first_fail_detail_id": state.first_fail_detail_id}


@app.get("/control/state")
async def control_state() -> dict:
    return state.snapshot()
