"""pytest 公共夹具。

- 每个测试会话使用独立的临时 SQLite 文件
- 在后台线程启动真实 uvicorn 跑模拟公示站（走真实 HTTP，127.0.0.1 回环）
- 采集端通过 HTTP 直连该模拟站，不访问任何外网
"""

from __future__ import annotations

import asyncio
import os
import socket
import sys
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

import httpx
import pytest
import uvicorn

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

_TMP_DB = os.path.join(tempfile.mkdtemp(prefix="collector-test-"), "test.db")
os.environ["DB_PATH"] = _TMP_DB
os.environ.setdefault("MAX_INFLIGHT", "2")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _start_mock_server():
    """在后台线程启动一个独立模拟站（独立 ControlState），返回 (base_url, server, thread)。"""
    from mock_site.app import create_app

    port = _free_port()
    config = uvicorn.Config(create_app(), host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)

    def _serve() -> None:
        # 专用线程 + 独立事件循环，避免与 pytest-asyncio 的循环串扰
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(server.serve())

    thread = threading.Thread(target=_serve, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{port}"
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"{base}/health", timeout=1)
            break
        except OSError:
            time.sleep(0.1)
    else:
        raise RuntimeError("mock site failed to start")
    return base, server, thread


@pytest.fixture(scope="session")
def mock_base_url() -> str:
    base, server, thread = _start_mock_server()
    os.environ["MOCK_BASE_URL"] = base
    yield base
    server.should_exit = True
    thread.join(timeout=10)
    assert not thread.is_alive(), "mock server thread did not stop"


@pytest.fixture(scope="session")
def mock_base_url_2() -> str:
    """第二个独立模拟站：与 mock_base_url 不共享控制状态/计数器。"""
    base, server, thread = _start_mock_server()
    yield base
    server.should_exit = True
    thread.join(timeout=10)
    assert not thread.is_alive(), "mock server thread did not stop"


@pytest.fixture(scope="function")
async def client(mock_base_url):
    """每用例独立的采集引擎 + httpx 客户端 + 干净的模拟站状态。"""
    from collector.db import db
    from collector.engine import engine

    # db 单例在导入时读取 DB_PATH，这里显式把它指向每用例独立的临时库，
    # 避免用例之间的数据累积污染
    db_dir = tempfile.mkdtemp(prefix="collector-case-")
    db.path = os.path.join(db_dir, "test.db")

    # 重置模拟站控制状态
    async with httpx.AsyncClient(base_url=mock_base_url, timeout=10) as mock_http:
        await mock_http.post("/control/reset")

    await db.connect()
    await engine.startup()
    from collector.app import app as collector_app

    transport = httpx.ASGITransport(app=collector_app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver", timeout=20
    ) as api:
        yield api, engine, db, mock_base_url
    await engine.shutdown()
    await db.close()


async def poll_progress(engine, task_id: str, expect_any: set[str], timeout: float = 30) -> dict:
    """轮询任务进度直到状态进入 expect_any。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        prog = await engine.get_progress(task_id)
        if prog["status"] in expect_any:
            return prog
        await asyncio.sleep(0.05)
    raise TimeoutError(f"task {task_id} did not reach {expect_any}")


async def wait_until(condition, timeout: float = 10, interval: float = 0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if await condition():
            return
        await asyncio.sleep(interval)
    raise TimeoutError("condition not met in time")
