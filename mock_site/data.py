"""固定的模拟公示数据集。

35 个唯一施工许可（PERMIT-001 .. PERMIT-035）。
目录列表按每页 10 条分页，并额外插入 3 条跨页重复条目，
使目录总条目为 38 条、共 4 页，用来验证采集端按许可号去重。
"""

from __future__ import annotations

PAGE_SIZE = 10

_DISTRICTS = ["朝阳区", "海淀区", "西城区", "东城区", "丰台区", "通州区"]
_STREETS = ["建设路", "和平大道", "解放街", "迎宾南路", "学院北路", "朝阳大街"]
_PROJECT_TYPES = [
    "住宅楼",
    "商业综合体",
    "学校教学楼",
    "医院综合楼",
    "市政道路",
    "产业园区厂房",
    "地下管廊",
    "体育场馆",
]


def _build_permits() -> list[dict]:
    permits: list[dict] = []
    for i in range(1, 36):
        district = _DISTRICTS[i % len(_DISTRICTS)]
        street = _STREETS[(i * 3) % len(_STREETS)]
        ptype = _PROJECT_TYPES[i % len(_PROJECT_TYPES)]
        year = 2025 + (i // 20)  # 前 19 个 2025 年，后面 2026 年
        month = (i % 12) + 1
        day = (i * 7 % 28) + 1
        permits.append(
            {
                "permit_no": f"PERMIT-{i:03d}",
                "project_name": f"{district}{ptype}项目{i:02d}期",
                "address": f"{district}{street}{(i % 90) + 10}号",
                "issue_date": f"{year}-{month:02d}-{day:02d}",
            }
        )
    return permits


PERMITS: list[dict] = _build_permits()
PERMIT_BY_NO: dict[str, dict] = {p["permit_no"]: p for p in PERMITS}

# 目录条目：(permit_no, detail_id)。前 35 条一一对应，随后追加 3 条重复，
# 重复条目指向已有许可，模拟不同目录页/批次里重复公示同一张许可。
CATALOG_ENTRIES: list[tuple[str, str]] = [
    (p["permit_no"], f"DET-{i + 1:03d}") for i, p in enumerate(PERMITS)
]
CATALOG_ENTRIES += [
    ("PERMIT-004", "DET-DUP-004"),
    ("PERMIT-017", "DET-DUP-017"),
    ("PERMIT-031", "DET-DUP-031"),
]

TOTAL_PAGES = (len(CATALOG_ENTRIES) + PAGE_SIZE - 1) // PAGE_SIZE


def catalog_page(page: int) -> dict | None:
    """返回第 page 页（1 起）的目录数据；越界返回 None。"""
    if page < 1 or page > TOTAL_PAGES:
        return None
    start = (page - 1) * PAGE_SIZE
    end = start + PAGE_SIZE
    items = [
        {"detail_id": detail_id, "permit_no": permit_no}
        for permit_no, detail_id in CATALOG_ENTRIES[start:end]
    ]
    return {
        "page": page,
        "page_size": PAGE_SIZE,
        "total_pages": TOTAL_PAGES,
        "total_items": len(CATALOG_ENTRIES),
        "items": items,
    }


def detail(detail_id: str) -> dict | None:
    """按目录里的 detail_id 返回详情；不存在返回 None。"""
    for permit_no, did in CATALOG_ENTRIES:
        if did == detail_id:
            base = PERMIT_BY_NO[permit_no]
            return {
                "detail_id": detail_id,
                "permit_no": base["permit_no"],
                "project_name": base["project_name"],
                "address": base["address"],
                "issue_date": base["issue_date"],
            }
    return None
