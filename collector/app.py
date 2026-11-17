"""采集 API 服务入口。

路由
----
POST   /api/tasks/start             启动采集任务
POST   /api/tasks/{id}/pause        暂停（响应返回后旧轮次不得再写入）
POST   /api/tasks/{id}/resume       恢复（只补采未成功项；并发恢复只启动一轮）
GET    /api/tasks/{id}/progress     查询进度
GET    /api/tasks                   任务列表
GET    /api/tasks/{id}/records      查询入库记录（可按 status 过滤、分页）
GET    /health                      健康检查
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query

from .db import db
from .engine import engine
from .schemas import ActionResponse, PermitRecord, StartRequest, TaskCreated, TaskProgress


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    await engine.startup()
    try:
        yield
    finally:
        await engine.shutdown()
        await db.close()


app = FastAPI(title="施工许可采集 API", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.post("/api/tasks/start", response_model=TaskCreated, status_code=201)
async def start_task(req: StartRequest | None = None) -> TaskCreated:
    target_url = req.target_url if req is not None else None
    result = await engine.start_task(target_url)
    return TaskCreated(**result)


@app.post("/api/tasks/{task_id}/pause", response_model=ActionResponse)
async def pause_task(task_id: str) -> ActionResponse:
    try:
        result = await engine.pause_task(task_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="task not found")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return ActionResponse(**result)


@app.post("/api/tasks/{task_id}/resume", response_model=ActionResponse)
async def resume_task(task_id: str) -> ActionResponse:
    try:
        result = await engine.resume_task(task_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="task not found")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except RuntimeError as exc:
        # 同任务并发恢复/任务运行中再次恢复
        raise HTTPException(status_code=409, detail=str(exc))
    return ActionResponse(**result)


@app.get("/api/tasks/{task_id}/progress", response_model=TaskProgress)
async def task_progress(task_id: str) -> TaskProgress:
    try:
        result = await engine.get_progress(task_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="task not found")
    return TaskProgress(**result)


@app.get("/api/tasks")
async def list_tasks() -> dict:
    tasks = await db.list_tasks()
    return {"items": tasks}


@app.get("/api/tasks/{task_id}/records", response_model=dict)
async def task_records(
    task_id: str,
    status: str | None = Query(default=None, pattern="^(pending|success|failed)$"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict:
    task = await db.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    rows = await db.list_permits(task_id, status=status, limit=limit, offset=offset)
    total = await db.count_permits(task_id, status=status)
    return {
        "task_id": task_id,
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": [PermitRecord(**row).model_dump() for row in rows],
    }
