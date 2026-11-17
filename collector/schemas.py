"""API 请求/响应模型。"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class StartRequest(BaseModel):
    target_url: Optional[str] = Field(
        default=None, description="公示站基础地址，缺省用服务端配置 MOCK_BASE_URL"
    )


class TaskProgress(BaseModel):
    task_id: str
    status: str
    target_url: str
    run_id: Optional[str]
    total_unique_permits: int
    success: int
    failed: int
    pending: int
    inflight: int
    done: bool
    last_error: Optional[str]
    catalog_pages_done: int
    catalog_total_pages: Optional[int]
    started_at: str
    updated_at: str


class PermitRecord(BaseModel):
    permit_no: str
    project_name: str
    address: str
    issue_date: str
    source_detail_id: str
    source_pages: str
    status: str
    attempts: int
    last_error: Optional[str]
    updated_at: str


class TaskCreated(BaseModel):
    task_id: str
    status: str
    run_id: str


class ActionResponse(BaseModel):
    task_id: str
    status: str
    run_id: Optional[str] = None
    message: str = ""
