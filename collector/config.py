"""采集服务配置，全部可通过环境变量覆盖。"""

from __future__ import annotations

import os


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


# 模拟公示站基础地址（docker compose 内用服务名 mock-site）
MOCK_BASE_URL = _env("MOCK_BASE_URL", "http://127.0.0.1:8001")

# SQLite 数据库文件
DB_PATH = _env("DB_PATH", "/workspace/data/collector.db")

# 每个采集任务允许的最大在途 HTTP 请求数
MAX_INFLIGHT = int(_env("MAX_INFLIGHT", "2"))

# 目录请求重试次数（目录是完整采集的前提）
CATALOG_MAX_ATTEMPTS = int(_env("CATALOG_MAX_ATTEMPTS", "3"))

# 单个详情在一轮采集中的尝试次数（失败项留给下一轮恢复时补采）
DETAIL_MAX_ATTEMPTS = int(_env("DETAIL_MAX_ATTEMPTS", "1"))

HTTP_TIMEOUT = float(_env("HTTP_TIMEOUT", "15"))
